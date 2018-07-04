package ru.fix.stdlib.ratelimiter;

import javafx.scene.paint.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.commons.profiler.PrefixedProfiler;
import ru.fix.commons.profiler.ProfiledCall;
import ru.fix.commons.profiler.Profiler;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Dispatcher class which manages tasks execution with given rate. Queues and
 * executes all task in single processor thread.
 */
public class RateLimitedDispatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String QUEUE_SIZE_INDICATOR = "queue_size";

    private final AtomicBoolean isAlive = new AtomicBoolean(true);

    private final RateLimiter rateLimiter;
    private final LinkedBlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final Thread thread;
    private final long limitAcquireTimeoutMillis;

    private final Profiler profiler;

    /**
     * Creates new dispatcher instance
     *
     * @param name        name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter rate limiter, which provides rate of operations
     */
    public RateLimitedDispatcher(String name, RateLimiter rateLimiter, Profiler profiler) {
        this(name, rateLimiter, profiler, Duration.of(30, ChronoUnit.SECONDS).toMillis());
    }

    /**
     * Creates new dispatcher instance
     *
     * @param name        name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter rate limiter, which provides rate of operations
     * @param limitAcquireTimeoutOnShutdownMillis prevents dispatcher from hanging on shutdown.
     *                                            If dispatcher is in shutdown state and
     *                                            can't acquire permit from RateLimiter in this timeout
     *                                            it will complete exceptionally all remaining operations
     */
    public RateLimitedDispatcher(String name, RateLimiter rateLimiter, Profiler profiler,
                                 long limitAcquireTimeoutOnShutdownMillis) {
        this.rateLimiter = rateLimiter;
        this.limitAcquireTimeoutMillis = limitAcquireTimeoutOnShutdownMillis;

        this.profiler = new PrefixedProfiler(profiler, "RateLimiterDispatcher." + name + ".");

        thread = new Thread(new TaskProcessor(), "rate-limited-dispatcher-" + name);
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

        if (!isAlive.get()) {
            RejectedExecutionException ex = new RejectedExecutionException(
                "RateLimiterDispatcher is in shutdown state"
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
        this.close(false);
    }

    public void close(boolean gracefully) throws Exception {
        isAlive.compareAndSet(true, false);

        if (!gracefully) {
            Task task;
            while ((task = queue.poll()) != null) {
                CancellationException ex = new CancellationException(
                    "Operation is cancelled because Dispatcher is shutting down." +
                        " If you want to complete already submitted operations on timeout use graceful shutdown close."
                );
                task.getFuture().completeExceptionally(ex);
                task.getQueueWaitTime().close();
            }
        }

        thread.interrupt();
        thread.join();

        rateLimiter.close();
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR);
    }

    @SuppressWarnings({"unchecked"})
    private final class TaskProcessor implements Runnable {

        @Override
        public void run() {
            try {
                while (isAlive.get() || !queue.isEmpty()) {
                    try {
                        processingCycle();
                    }
                    catch (StopProcessingException e) {
                        throw e;
                    }
                    catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            catch (StopProcessingException e) {
                queue.forEach(task -> {
                    task.getFuture().completeExceptionally(e);
                    task.getQueueWaitTime().close();
                });
            }
        }

        private void processingCycle() throws StopProcessingException {
            Task task;
            try {
                task = queue.take();
            }
            catch (InterruptedException e) {
                if (isAlive.get()) {
                    logger.error("RateLimitedDispatchers processor thread was interrupted", e);
                }
                return;
            }

            task.getQueueWaitTime().stop();
            CompletableFuture future = task.getFuture();

            try {
                acquirePermit();

                Object result = profiler.profile(
                    "supplied_operation",
                    () -> task.getSupplier().get()
                );
                future.complete(result);
            }
            catch (StopProcessingException e) {
                future.completeExceptionally(e);
                throw e;
            }
            catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        private void acquirePermit() throws StopProcessingException {
            try (ProfiledCall limitAcquireTime = profiler.start("acquire_limit")) {

                boolean acquired = rateLimiter.tryAcquire(limitAcquireTimeoutMillis, ChronoUnit.MILLIS);

                while (isAlive.get() && !acquired) {
                    acquired = rateLimiter.tryAcquire(limitAcquireTimeoutMillis, ChronoUnit.MILLIS);
                }
                if (!acquired) {
                    logger.error(
                        "RateLimitedDispatcher is in shutdown state and can't acquire permit" +
                        " in " + limitAcquireTimeoutMillis + "ms." +
                        " All remaining operations will be completed exceptionally"
                    );
                    throw new StopProcessingException(
                        "RateLimitedDispatcher is in shutdown state and couldn't acquire permit" +
                            " in " + limitAcquireTimeoutMillis + "ms."
                    );
                }
                limitAcquireTime.stop();
            }
        }
    }

    private static final class Task<T> {
        private final Supplier<T> supplier;
        private final CompletableFuture<T> future;

        private final ProfiledCall queueWaitTime;

        @SuppressWarnings("WeakerAccess")
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

    @SuppressWarnings("serial")
    private static class StopProcessingException extends Exception {

        public StopProcessingException(String message) {
            super(message);
        }
    }
}