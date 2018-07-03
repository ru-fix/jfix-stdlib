package ru.fix.stdlib.ratelimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.commons.profiler.PrefixedProfiler;
import ru.fix.commons.profiler.ProfiledCall;
import ru.fix.commons.profiler.Profiler;

import java.lang.invoke.MethodHandles;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
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
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final Thread thread;

    private final Profiler profiler;

    /**
     * Creates new dispatcher instance
     *
     * @param name        name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter rate limiter, which provides rate of operations
     */
    public RateLimitedDispatcher(String name, RateLimiter rateLimiter, Profiler profiler) {
        this.rateLimiter = rateLimiter;
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
        isAlive.compareAndSet(true, false);
        rateLimiter.close();
        if (!queue.isEmpty()) {
            logger.error("RateLimitedDispatcher queue is not empty. {} elements left unprocessed", queue.size());
            queue.forEach(task -> task.getQueueWaitTime().stop());
        }
        thread.interrupt();
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR);
    }

    @SuppressWarnings({"unchecked"})
    private final class TaskProcessor implements Runnable {

        @Override
        public void run() {
            while (isAlive.get()) {
                try {
                    Task task = queue.take();
                    task.getQueueWaitTime().stop();

                    CompletableFuture future = task.getFuture();
                    try (ProfiledCall limitAcquireTime = profiler.start("acquire_limit")) {
                        rateLimiter.tryAcquire(5, ChronoUnit.MINUTES);
                        limitAcquireTime.stop();

                        Object result = profiler.profile(
                            "supplied_operation",
                            () -> task.getSupplier().get()
                        );

                        future.complete(result);
                    }
                    catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }
                catch (InterruptedException e1) {
                    if (isAlive.get()) {
                        logger.error("RateLimitedDispatchers processor thread was interrupted", e1);
                    }
                }
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

}