package ru.fix.stdlib.ratelimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.NoopProfiler;
import ru.fix.aggregating.profiler.PrefixedProfiler;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.lang.invoke.MethodHandles;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
    private final LinkedBlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final Thread thread;

    private final String name;
    private final Profiler profiler;

    private final DynamicProperty<Long> closingTimeout;

    /**
     * Creates new dispatcher instance
     *
     * @param name           name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter    rate limiter, which provides rate of operation
     * @param closingTimeout max amount of time (in milliseconds) for waiting pending operations.
     *                       If parameter equals 0 it means that operations was completed immediately.
     *                       Any negative number will be interpreted as 0.
     */
    public RateLimitedDispatcher(String name,
                                 RateLimiter rateLimiter,
                                 Profiler profiler,
                                 DynamicProperty<Long> closingTimeout) {
        this.name = name;
        this.rateLimiter = rateLimiter;
        this.closingTimeout = closingTimeout;

        this.profiler = new PrefixedProfiler(profiler, "RateLimiterDispatcher." + name + ".");

        thread = new Thread(new TaskProcessor(), "rate-limited-dispatcher-" + name);

        state.set(State.RUNNING);
        thread.start();

        this.profiler.attachIndicator(QUEUE_SIZE_INDICATOR, () -> (long) queue.size());
    }

    public <T> CompletableFuture<T> compose(Supplier<CompletableFuture<T>> supplier) {
        return submit(supplier).thenComposeAsync(cf -> cf);
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
    public <T> CompletableFuture<T> submit(Supplier<T> supplier) {
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
        queue.add(new Task(result, supplier, queueWaitTime));

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
        // If queue is empty this will awake waiting Thread
        queue.add(new PoisonPillTask());

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

    @SuppressWarnings({"unchecked"})
    private final class TaskProcessor implements Runnable {

        @Override
        public void run() {
            while (state.get() == State.RUNNING ||
                    state.get() == State.SHUTTING_DOWN && !queue.isEmpty()) {
                try {
                    processingCycle();
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

            queue.forEach(task -> {
                task.getFuture().completeExceptionally(new RejectedExecutionException(taskExceptionText));
                task.getQueueWaitTime().close();
            });

        }

        private void processingCycle() throws InterruptedException {
            Task task = queue.take();

            if (task instanceof PoisonPillTask) {
                return;
            }

            task.getQueueWaitTime().stop();
            CompletableFuture future = task.getFuture();

            try {
                try (ProfiledCall limitAcquireTime = profiler.start("acquire_limit")) {

                    boolean acquired = false;
                    while (state.get() != State.TERMINATE && !acquired) {
                        acquired = rateLimiter.tryAcquire(1, ChronoUnit.SECONDS);
                    }
                    if (!acquired) {
                        future.completeExceptionally(new RejectedExecutionException(
                                "RateLimitedDispatcher [" + name + "] is in [TERMINATE] state"
                        ));
                        return;
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

    private static class PoisonPillTask extends Task<Void> {
        public PoisonPillTask() {
            super(new CompletableFuture<>(), () -> null, new NoopProfiler.NoopProfiledCall());
        }
    }

    private enum State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }
}