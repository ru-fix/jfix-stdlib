package ru.fix.stdlib.concurrency.futures;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        /**
         * Create limiter with 3 pending tasks max
         */
        PendingFutureLimiter limiter = new PendingFutureLimiter(3);

        /**
         * blockingEnqueue 3 tasks without blocking
         */
        for (int i = 0; i < 3; i++) {
            limiter.enqueueBlocking(createTask());
        }

        /**
         * Currently there are 3 pending operations
         */
        assertEquals(3, limiter.getPendingCount());

        /**
         * Global counter does not changed - background task have not completed yet
         */
        assertEquals(0, globalCounter.get());

        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            startAllTasks();
        }, 1, TimeUnit.SECONDS);

        /**
         * Enqueue 4-ht task, Should block until one of previous task will complete
         */
        limiter.enqueueBlocking(createTask());

        /**
         * At least one task should be completed
         */
        assertTrue(globalCounter.get() >= 1);


        /**
         * All tasks should complete
         */
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> globalCounter.get() == 4 && limiter.getPendingCount() == 0);

        assertEquals(4, globalCounter.get());
        assertEquals(0, limiter.getPendingCount());
    }

    public class SessionStub {
        private volatile boolean readable = true;

        public boolean isReadable() {
            return this.readable;
        }

        public void setReadable(boolean readable) {
            this.readable = readable;
        }
    }

    @Test
    public void enqueue_channelBlockReading_blockUnblock() throws Exception {
        SessionStub sessionStub = new SessionStub();
        PendingFutureLimiter limiter = new PendingFutureLimiter(3);
        limiter.setTresspassingThresholdListener(
                new PendingFutureLimiter.ThresholdListener() {
                    @Override
                    public void onLimitReached() {
                        sessionStub.setReadable(false);
                    }

                    @Override
                    public void onLimitSubceed() {
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

    private void startAllTasks() {
        startTasksFlag.set(true);
        synchronized (startTasksFlag) {
            startTasksFlag.notifyAll();
        }
    }

    @Test
    public void limiter_pending_count_reset_on_future_with_handler() throws Exception {

        /**
         * Create limiter with 3 pending tasks max
         */
        PendingFutureLimiter limiter = new PendingFutureLimiter(3);

        CompletableFuture<Void> future = createTask();
        future.handleAsync((any, exc) -> {
            log.info("handleAsync");
            return any;
        });

        startAllTasks();

        limiter.enqueueBlocking(future);

        /**
         * All tasks should complete
         */
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> globalCounter.get() == 1 && limiter.getPendingCount() == 0);

        assertEquals(1, globalCounter.get());
        assertEquals(0, limiter.getPendingCount());
    }

}
