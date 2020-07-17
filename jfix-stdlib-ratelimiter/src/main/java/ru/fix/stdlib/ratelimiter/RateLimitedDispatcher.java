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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Manages tasks execution with given rate and window.
 * Rate specify how many requests per second will be dispatched.
 * Window size specify how many async operations with uncompleted result allowed.
 * When Window or Rate restriction is reached, dispatcher will stop to process requests and enqueue them in umbound queue.
 * Disaptcher executes all operations in single dedicated thread.
 */
public class RateLimitedDispatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String QUEUE_SIZE_INDICATOR = "queue_size";
    private static final String ACTIVE_ASYNC_OPERATIONS = "active_async_operations";

    private final AtomicReference<State> state = new AtomicReference<>();

    private final RateLimiter rateLimiter;
    private final LinkedBlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Command> commandQueue = new LinkedBlockingQueue<>();

    private final Thread thread;
    private final String name;
    private final Profiler profiler;

    /**
     * Accessed only by single dedicated thread
     */
    private int windowSize = 0;
    private final DynamicProperty<Long> closingTimeout;

    private final Semaphore windowSemaphore;
    private final PropertySubscription<Integer> windowSizeSubscription;

    /**
     * How many async operations executed but their results are not ready yet.
     */
    private final AtomicInteger activeAsyncOperations = new AtomicInteger();


    /**
     * Creates new dispatcher instance
     *
     * @param name           name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter    rate limiter, which provides rate of operation
     * @param windowSize     ho many async operations with uncompleted result are allowed in dispatcher
     * @param closingTimeout max amount of time (in milliseconds) to wait for pending operations during shutdown.
     *                       If parameter equals 0 then dispatcher will not wait pending operations during closing process.
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
        this.profiler.attachIndicator(ACTIVE_ASYNC_OPERATIONS, () -> (long) activeAsyncOperations.get());

        this.windowSizeSubscription = windowSize.createSubscription().setAndCallListener((oldValue, newValue) -> {
            submitCommand(new ChangeWindowSizeCommand(oldValue != null ? oldValue : 0, newValue));
        });
    }

    private void asyncOperationStarted(){
        activeAsyncOperations.incrementAndGet();
    }

    private void asyncOperationCompleted(){
        activeAsyncOperations.decrementAndGet();
        windowSemaphore.release();
    }

    private void submitCommand(Command command) {
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
                        .whenComplete((t, throwable) -> asyncOperationCompleted())
        ).thenComposeAsync(cf -> cf);
    }

    public <AsyncResultT> CompletableFuture<AsyncResultT> compose(
            AsyncOperation<AsyncResultT> asyncOperation,
            AsyncResultSubscriber<AsyncResultT> asyncResultSubscriber
    ) {
        return submit(() -> {
            AsyncResultT asyncResult = asyncOperation.invoke();
            asyncResultSubscriber.subscribe(asyncResult, this::asyncOperationCompleted);
            return asyncResult;
        });
    }

    /**
     * Submits new operation to task queue.
     * <p>
     * WARNING: task should not be long running operation
     * and should not block processing thread.
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
                    "RateLimiterDispatcher [" + name + "] is in '" + state + "' state"
            );
            result.completeExceptionally(ex);
            return result;
        }

        ProfiledCall queueWaitTime = profiler.start("queue_wait");
        taskQueue.add(new Task<>(result, supplier, queueWaitTime));

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
        profiler.detachIndicator(ACTIVE_ASYNC_OPERATIONS);
    }


    /**
     * Async operation that should be invoked by {@link RateLimitedDispatcher} at configurate rate.
     * @param <AsyncResultT> represents result of async operation
     */
    @FunctionalInterface
    public interface AsyncOperation<AsyncResultT> {
        AsyncResultT invoke();
    }

    /**
     * Informs {@link RateLimitedDispatcher} that async operation result is completed.
     */
    @FunctionalInterface
    public interface AsyncResultCallback {
        /**
         * Invoke, when async operation result is completed successfully or with exception.
         */
        void onAsyncResultCompleted();
    }

    /**
     * Invoked by {@link RateLimitedDispatcher}.
     * Should attach asyncResultCallback to asyncResult and invoke it when asyncResult is complete.
     * @param <AsyncResultT> represents result of async operation
     */
    @FunctionalInterface
    public interface AsyncResultSubscriber<AsyncResultT> {
        /**
         * Invoked by {@link RateLimitedDispatcher}.
         * Should attach asyncResultCallback to asyncResult and invoke it when asyncResult is complete.
         * @param asyncResult that will complete asynchronously
         * @param asyncResultCallback should be invoked when asyncResult is complete
         */
        void subscribe(AsyncResultT asyncResult, AsyncResultCallback asyncResultCallback);
    }

    private final class TaskProcessor implements Runnable {
        @Override
        public void run() {
            while (state.get() == State.RUNNING ||
                    (state.get() == State.SHUTTING_DOWN && !taskQueue.isEmpty())) {
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
                taskExceptionText = "RateLimitedDispatcher [" + name + "] interrupted";
            }

            taskQueue.forEach(task -> {
                task.getFuture().completeExceptionally(new RejectedExecutionException(taskExceptionText));
                task.getQueueWaitTimeCall().close();
            });

        }

        private void processCommandsIfExist() throws InterruptedException{
            for (Command command = commandQueue.poll();
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

            task.getQueueWaitTimeCall().stop();
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
                        "supply_operation",
                        () -> task.getSupplier().get()
                );
                asyncOperationStarted();

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

        private final ProfiledCall queueWaitTimeCall;

        public Task(CompletableFuture<T> future, Supplier<T> supplier, ProfiledCall queueWaitTimeCall) {
            this.future = future;
            this.supplier = supplier;
            this.queueWaitTimeCall = queueWaitTimeCall;
        }

        public Supplier<T> getSupplier() {
            return supplier;
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }

        public ProfiledCall getQueueWaitTimeCall() {
            return queueWaitTimeCall;
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