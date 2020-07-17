package ru.fix.stdlib.ratelimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.NoopProfiler;
import ru.fix.aggregating.profiler.PrefixedProfiler;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;

import java.lang.invoke.MethodHandles;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Dispatcher class which manages tasks execution with given rate. Queues and
 * executes all task in single processor thread.
 */
public class RateLimitedDispatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String QUEUE_SIZE_INDICATOR = "queue_size";

    private final AtomicReference<State> state = new AtomicReference<>();

    private final RateLimiter rateLimiter;
    private final LinkedBlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ChangeWindowSizeCommand> commandQueue = new LinkedBlockingQueue<>();

    private final Thread thread;

    private final String name;
    private final Profiler profiler;

    /**
     * Accessed only by single thread
     */
    private int windowSize = 0;
    private final DynamicProperty<Long> closingTimeout;

    private final Semaphore windowSemaphore;
    private final PropertySubscription<Integer> windowSizeSubscription;



    /**
     * Creates new dispatcher instance
     *
     * @param name           name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter    rate limiter, which provides rate of operation
     * @param windowSize     max amount of concurrently executing tasks
     * @param closingTimeout max amount of time (in milliseconds) for waiting pending operations.
     *                       If parameter equals 0 it means that operations was completed immediately.
     *                       Any negative number will be interpreted as 0.
     */
    public RateLimitedDispatcher(String name,
                                 RateLimiter rateLimiter,
                                 Profiler profiler,
                                 DynamicProperty<Integer> windowSize,
                                 DynamicProperty<Long> closingTimeout) {
        this.name = name;
        this.rateLimiter = rateLimiter;
        this.closingTimeout = closingTimeout;
        this.windowSemaphore = new Semaphore(0);

        this.profiler = new PrefixedProfiler(profiler, "RateLimiterDispatcher." + name + ".");

        thread = new Thread(new TaskProcessor(), "rate-limited-dispatcher-" + name);

        state.set(State.RUNNING);
        thread.start();

        this.profiler.attachIndicator(QUEUE_SIZE_INDICATOR, () -> (long) taskQueue.size());

        this.windowSizeSubscription = windowSize.createSubscription().setAndCallListener((oldValue, newValue) -> {
            submitCommand(new ChangeWindowSizeCommand(oldValue != null ? oldValue : 0, newValue));
        });
    }

    private void submitCommand(ChangeWindowSizeCommand command) {
        commandQueue.add(command);
        taskQueue.add(new AwakeFromWaitingQueueTask());
    }

    public RateLimitedDispatcher(String name,
                                 RateLimiter rateLimiter,
                                 Profiler profiler,
                                 DynamicProperty<Long> closingTimeout) {
        this(name, rateLimiter, profiler, DynamicProperty.of(0), closingTimeout);
    }

    public <T> CompletableFuture<T> compose(Supplier<CompletableFuture<T>> supplier) {
        return submit(
                () -> supplier.get()
                        .whenComplete((t, throwable) -> windowSemaphore.release())
        ).thenComposeAsync(cf -> cf);
    }

    public <AsyncResultT> CompletableFuture<AsyncResultT> compose(
            AsyncOperation<AsyncResultT> asyncOperation,
            AsyncResultSubscriber<AsyncResultT> asyncResultSubscriber
    ) {
        return submit(() -> {
            AsyncResultT asyncResult = asyncOperation.invoke();
            asyncResultSubscriber.subscribe(asyncResult, windowSemaphore::release);
            return asyncResult;
        });
    }

    /**
     * Submits task.
     * <p>
     * !WARN. Task should not be long running operation and should not block
     * processing thread.
     *
     * @param supplier task to execute and retrieve result
     * @return feature which represent result of task execution
     */
    @SuppressWarnings({"unchecked"})
    private <T> CompletableFuture<T> submit(Supplier<T> supplier) {
        CompletableFuture<T> result = new CompletableFuture<>();

        State state = this.state.get();
        if (state != State.RUNNING) {
            RejectedExecutionException ex = new RejectedExecutionException(
                    "RateLimiterDispatcher [" + name + "] is in [" + state + "] state"
            );
            result.completeExceptionally(ex);
            return result;
        }

        @SuppressWarnings("resource")
        ProfiledCall queueWaitTime = profiler.start("queue_wait");
        taskQueue.add(new Task(result, supplier, queueWaitTime));

        return result;
    }

    public void updateRate(int rate) {
        rateLimiter.updateRate(rate);
    }

    @Override
    public void close() throws Exception {
        boolean stateUpdated = state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN);
        if (!stateUpdated) {
            logger.info("Close called on RateLimitedDispatcher [{}] with state [{}]", name, state.get());
            return;
        }
        windowSizeSubscription.close();

        // If queue is empty this will awake waiting Thread
        taskQueue.add(new AwakeFromWaitingQueueTask());

        if (closingTimeout.get() < 0) {
            logger.warn("Rate limiter timeout must be greater than or equals 0. Current value is {}, rate limiter name: {}",
                    closingTimeout.get(), name);
        }
        long timeout = Math.max(closingTimeout.get(), 0);

        if (timeout > 0) {
            thread.join(timeout);
        }

        stateUpdated = state.compareAndSet(State.SHUTTING_DOWN, State.TERMINATE);
        if (!stateUpdated) {
            logger.error(
                    "Can't set [TERMINATE] state to RateLimitedDispatcher [{}] in [{}] state",
                    name, state.get()
            );
            return;
        }
        thread.join();

        rateLimiter.close();
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR);
    }


    @FunctionalInterface
    interface AsyncOperation<AsyncResultT> {
        AsyncResultT invoke();
    }

    @FunctionalInterface
    public interface AsyncResultCallback {
        /**
         * Async operation is complete successfully or with exception.
         */
        void onAsyncResultCompleted();
    }

    @FunctionalInterface
    public interface AsyncResultSubscriber<AsyncResultT> {
        void subscribe(AsyncResultT asyncResult, AsyncResultCallback asyncResultCallback);
    }

    @SuppressWarnings({"unchecked"})
    private final class TaskProcessor implements Runnable {

        @Override
        public void run() {
            while (state.get() == State.RUNNING ||
                    state.get() == State.SHUTTING_DOWN && !taskQueue.isEmpty()) {
                try {
                    processCommandsIfExist();
                    waitForTaskInQueueAndProcess();

                } catch (InterruptedException interruptedException) {
                    logger.error(interruptedException.getMessage(), interruptedException);
                    break;

                } catch (Exception otherException) {
                    logger.error(otherException.getMessage(), otherException);
                }
            }

            String taskExceptionText;
            if (state.get() == State.TERMINATE) {
                taskExceptionText = "RateLimitedDispatcher [" + name + "] is in [TERMINATE] state";
            } else {
                taskExceptionText = "RateLimitedDispatcher [" + name + "] was interrupted";
            }

            taskQueue.forEach(task -> {
                task.getFuture().completeExceptionally(new RejectedExecutionException(taskExceptionText));
                task.getQueueWaitTime().close();
            });

        }

        private void processCommandsIfExist() throws InterruptedException{
            for (ChangeWindowSizeCommand command = commandQueue.poll();
                 command != null;
                 command = commandQueue.poll()) {

                command.apply();
            }
        }

        private void waitForTaskInQueueAndProcess() throws InterruptedException {
            Task task = taskQueue.take();

            if (task instanceof AwakeFromWaitingQueueTask) {
                return;
            }

            task.getQueueWaitTime().stop();
            CompletableFuture future = task.getFuture();

            try {
                if (windowSize > 0) {
                    try (ProfiledCall acquireWindowTime = profiler.start("acquire_window")) {
                        boolean windowAcquired = false;
                        while (!windowAcquired) {
                            if (state.get() == State.TERMINATE) {
                                rejectDueToTerminateState(future);
                                return;
                            }
                            windowAcquired = windowSemaphore.tryAcquire(3, TimeUnit.SECONDS);
                        }
                        acquireWindowTime.stop();
                    }
                }

                try (ProfiledCall limitAcquireTime = profiler.start("acquire_limit")) {

                    boolean limitAcquired = false;
                    while (!limitAcquired) {
                        if (state.get() == State.TERMINATE) {
                                rejectDueToTerminateState(future);
                                return;
                            }
                        limitAcquired = rateLimiter.tryAcquire(3, ChronoUnit.SECONDS);
                    }
                    limitAcquireTime.stop();
                }

                Object result = profiler.profile(
                        "supplied_operation",
                        () -> task.getSupplier().get()
                );
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        private void rejectDueToTerminateState(CompletableFuture<?> future) {
            future.completeExceptionally(new RejectedExecutionException(
                    "RateLimitedDispatcher [" + name + "] is in [TERMINATE] state"
            ));
        }
    }

    private static class Task<T> {
        private final Supplier<T> supplier;
        private final CompletableFuture<T> future;

        private final ProfiledCall queueWaitTime;

        public Task(CompletableFuture<T> future, Supplier<T> supplier, ProfiledCall queueWaitTime) {
            this.future = future;
            this.supplier = supplier;
            this.queueWaitTime = queueWaitTime;
        }

        public Supplier<T> getSupplier() {
            return supplier;
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }

        public ProfiledCall getQueueWaitTime() {
            return queueWaitTime;
        }
    }

    private static class AwakeFromWaitingQueueTask extends Task<Void> {
        public AwakeFromWaitingQueueTask() {
            super(new CompletableFuture<>(), () -> null, new NoopProfiler.NoopProfiledCall());
        }
    }

    interface Command{
        void apply() throws InterruptedException;
    }

    private class ChangeWindowSizeCommand implements Command {
        private final int oldSize;
        private final int newSize;

        public ChangeWindowSizeCommand(int oldSize, int newSize) {
            this.oldSize = oldSize;
            this.newSize = newSize;
        }

        @Override
        public void apply() throws InterruptedException {
            if(newSize == oldSize)
                return;

            windowSize = newSize;
            if(newSize > oldSize)
                windowSemaphore.release(newSize - oldSize);
            else
                windowSemaphore.acquire(oldSize - newSize);
        }
    }

    private enum State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }
}