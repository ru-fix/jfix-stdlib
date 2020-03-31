package ru.fix.stdlib.concurrency.futures;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kamil Asfandiyarov
 */
public class PendingFutureLimiterTest {

    private static final Logger log = LoggerFactory.getLogger(PendingFutureLimiterTest.class);
    private static final long FUTURE_LIMITER_TIMEOUT_MINUTES = 15;

    private final Executor executor = Executors.newFixedThreadPool(20);
    private final AtomicInteger globalCounter = new AtomicInteger();
    private final AtomicBoolean startTasksFlag = new AtomicBoolean();

    private CompletableFuture<Void> createTask() {
        return CompletableFuture.runAsync(() -> {
            try {
                synchronized (startTasksFlag) {
                    while (!startTasksFlag.get()) {
                        startTasksFlag.wait();
                    }
                }
                globalCounter.incrementAndGet();
            } catch (Exception exc) {
                log.error(exc.getMessage(), exc);
            }
        }, executor);
    }

    @BeforeEach
    public void clean() {
        globalCounter.set(0);
        startTasksFlag.set(false);
    }

    @Test
    public void block_on_task_when_pending_count_bigger_than_max_border() throws Exception {

        PendingFutureLimiter limiter = new LimiterBuilder()
                .enqueueTasks(3)
                .build();

        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        // Global counter does not changed - background task have not completed yet
        assertEquals(0, globalCounter.get());

        Executors.newSingleThreadScheduledExecutor().schedule(this::startAllTasks, 1, TimeUnit.SECONDS);

        // Enqueue 4-th task, Should block until one of previous task will complete
        limiter.enqueueBlocking(createTask());

        // At least one task should be completed
        assertTrue(globalCounter.get() >= 1);

        // All tasks should complete
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> globalCounter.get() == 4 && limiter.getPendingCount() == 0);

        assertEquals(4, globalCounter.get());
        assertEquals(0, limiter.getPendingCount());
    }

    @Test
    public void enqueue_should_pass_after_timout_when_it_is_more_than_zero() throws Exception {

        long executionTimeLimit = TimeUnit.SECONDS.toMillis(20);

        PendingFutureLimiter limiter = new LimiterBuilder()
                .executionTimeLimit(executionTimeLimit)
                .frequencyToCheckQueueSize(TimeUnit.SECONDS.toMillis(5))
                .enqueueTasks(3)
                .build();


        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            try {
                limiter.enqueueBlocking(createTask());
            } catch (InterruptedException ignore) {}
        }, executionTimeLimit, TimeUnit.MILLISECONDS);

        await().atMost(executionTimeLimit * 2, TimeUnit.MILLISECONDS)
                .until(() -> limiter.getPendingCount() == 1);
    }

    @Test
    public void waitAll_should_wait_till_futures_completed() throws Exception {
        long timeToCheckWait =  TimeUnit.SECONDS.toMillis(20);

        PendingFutureLimiter limiter = new LimiterBuilder()
                .executionTimeLimit(0)
                .frequencyToCheckQueueSize(TimeUnit.SECONDS.toMillis(5))
                .enqueueTasks(3)
                .build();

        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        long start = System.currentTimeMillis();

        //We'll start all tasks after some time...
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            startAllTasks();
        }, timeToCheckWait, TimeUnit.MILLISECONDS);
        limiter.waitAll();

        //And check if we've been waiting for this time and all tasks to complete
        assertTrue(System.currentTimeMillis() - start >= timeToCheckWait);
        assertEquals(globalCounter.get(), 3);
        assertEquals(limiter.getPendingCount(), 0);
    }

    @Test
    public void waitAll_should_wait_till_futures_are_timeouted() throws Exception {
        long executionTimeLimit = TimeUnit.SECONDS.toMillis(20);

        PendingFutureLimiter limiter = new LimiterBuilder()
                .executionTimeLimit(executionTimeLimit)
                .frequencyToCheckQueueSize(TimeUnit.SECONDS.toMillis(5))
                .enqueueTasks(3)
                .build();

        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        long start = System.currentTimeMillis();
        // We'll try to wait without starting any task...
        limiter.waitAll();
        // So we expect no task will be executed and the queue will be purged after timeout
        assertTrue(System.currentTimeMillis() - start >= executionTimeLimit);
        assertEquals(globalCounter.get(), 0);
        assertEquals(limiter.getPendingCount(), 0);
    }

    @Test
    public void waitAll_should_return_false_when_couldnt_release_futures_and_true_when_everything_is_completed() throws Exception {

        long timeToWait = TimeUnit.SECONDS.toMillis(20);
        // Create limiter with no timeout
        PendingFutureLimiter limiter = new LimiterBuilder()
                .executionTimeLimit(0)
                .frequencyToCheckQueueSize(TimeUnit.SECONDS.toMillis(5))
                .enqueueTasks(3)
                .build();

        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        long start = System.currentTimeMillis();

        // We'll try to wait without starting any task...
        assertFalse(limiter.waitAll(timeToWait));

        // So we expect no task will be executed and the queue will be still full after waiting time
        assertTrue(System.currentTimeMillis() - start >= timeToWait);
        assertEquals(globalCounter.get(), 0);
        assertEquals(limiter.getPendingCount(), 3);

        startAllTasks();
        // But after all tasks started - waitAll should return true
        assertTrue(limiter.waitAll(timeToWait));
        // And after that - all tasks are completed and the queue is free
        assertEquals(globalCounter.get(), 3);
        assertEquals(limiter.getPendingCount(), 0);
    }

    @Test
    public void waitAll_conditions_with_executionTimeLimited_limiter() throws Exception {
        long timeToWait = TimeUnit.SECONDS.toMillis(20);

        // Create limiter with timeout
        PendingFutureLimiter limiter = new LimiterBuilder()
                .executionTimeLimit(timeToWait * 2)
                .frequencyToCheckQueueSize(TimeUnit.SECONDS.toMillis(5))
                .enqueueTasks(3)
                .build();

        //Currently there are 3 pending operations
        assertEquals(3, limiter.getPendingCount());

        long start = System.currentTimeMillis();

        // We'll try to wait without starting any task...
        assertFalse(limiter.waitAll(timeToWait));

        // So we expect no task will be executed and the queue will be still full after waiting time
        assertTrue(System.currentTimeMillis() - start >= timeToWait);
        assertEquals(globalCounter.get(), 0);
        assertEquals(limiter.getPendingCount(), 3);

        // We won't start any task, just wait once more
        assertTrue(limiter.waitAll(timeToWait * 2));
        // And after that - no task is completed, but the queue is purged by timeout
        assertEquals(globalCounter.get(), 0);
        assertEquals(limiter.getPendingCount(), 0);
    }

    private class LimiterBuilder {
        private long executionTimeLimit = TimeUnit.MINUTES.toMillis(FUTURE_LIMITER_TIMEOUT_MINUTES);
        private long frequencyToCheckQueueSize = 0;
        private int tasksToEnqueue = 0;
        private int maxPendingCount = 3;

        LimiterBuilder() {}

        PendingFutureLimiter build() throws Exception {
            PendingFutureLimiter res = new PendingFutureLimiter(maxPendingCount, executionTimeLimit);

            if (frequencyToCheckQueueSize != 0) {
                Field waitTimeToCheckSizeQueue = PendingFutureLimiter.class.getDeclaredField("waitTimeToCheckSizeQueue");
                waitTimeToCheckSizeQueue.setAccessible(true);
                waitTimeToCheckSizeQueue.set(res, frequencyToCheckQueueSize);
            }

            if (tasksToEnqueue != 0) {
                for (int i = 0; i < tasksToEnqueue; i++) {
                    res.enqueueBlocking(createTask());
                }
            }

            return res;
        }

        LimiterBuilder executionTimeLimit(long executionTimeLimit) {
            this.executionTimeLimit = executionTimeLimit;
            return this;
        }

        LimiterBuilder frequencyToCheckQueueSize(long frequencyToCheckQueueSize) {
            this.frequencyToCheckQueueSize = frequencyToCheckQueueSize;
            return this;
        }

        LimiterBuilder enqueueTasks(int tasksToEnqueue) {
            this.tasksToEnqueue = tasksToEnqueue;
            return this;
        }

        LimiterBuilder maxPendingCount(int maxPendingCount) {
            this.maxPendingCount = maxPendingCount;
            return this;
        }

    }

    @Test
    public void enqueue_channelBlockReading_blockUnblock() throws Exception {
        SessionStub sessionStub = new SessionStub();
        PendingFutureLimiter limiter = new PendingFutureLimiter(3, TimeUnit.MINUTES.toMillis(FUTURE_LIMITER_TIMEOUT_MINUTES));
        limiter.setThresholdListener(
                new PendingFutureLimiter.ThresholdListener() {
                    @Override
                    public void onHiLimitReached() {
                        sessionStub.setReadable(false);
                    }

                    @Override
                    public void onLowLimitSubceed() {
                        sessionStub.setReadable(true);
                    }
                });
        for (int i = 0; i < 3; i++) {
            limiter.enqueueUnlimited(createTask());
        }
        assertEquals(3, limiter.getPendingCount());
        assertTrue(sessionStub.isReadable());

        limiter.enqueueUnlimited(createTask());
        assertFalse(sessionStub.isReadable());

        startAllTasks();
        with().pollDelay(10L, TimeUnit.MILLISECONDS)
                .until(() -> limiter.getPendingCount() == 0);
        with().pollDelay(0L, TimeUnit.MILLISECONDS)
                .atMost(500, TimeUnit.MILLISECONDS)
                .until(sessionStub::isReadable);
    }

    @Test
    public void limiter_pending_count_reset_on_future_with_handler() throws Exception {

        // Create limiter with 3 pending tasks max
        PendingFutureLimiter limiter = new PendingFutureLimiter(3, TimeUnit.MINUTES.toMillis(FUTURE_LIMITER_TIMEOUT_MINUTES));

        CompletableFuture<Void> future = createTask();
        future.handleAsync((any, exc) -> {
            log.info("handleAsync");
            return any;
        });

        startAllTasks();

        limiter.enqueueBlocking(future);

        // All tasks should complete
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> globalCounter.get() == 1 && limiter.getPendingCount() == 0);

        assertEquals(1, globalCounter.get());
        assertEquals(0, limiter.getPendingCount());
    }

    public static class SessionStub {
        private volatile boolean readable = true;

        public boolean isReadable() {
            return this.readable;
        }

        public void setReadable(boolean readable) {
            this.readable = readable;
        }
    }

    private void startAllTasks() {
        startTasksFlag.set(true);
        synchronized (startTasksFlag) {
            startTasksFlag.notifyAll();
        }
    }

}
