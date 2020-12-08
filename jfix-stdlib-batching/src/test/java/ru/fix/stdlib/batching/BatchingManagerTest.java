package ru.fix.stdlib.batching;

import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.NoopProfiler;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;


class BatchingManagerTest {

    @Test
    void testBlockingMode() throws InterruptedException {
        try (BatchingManager<Object, Object, String> batchingManager = createDumbBatchingManager(10, 1000, true, 1000)) {
            String key = "key1";
            for (int i = 0; i < 30; i++) {
                try {
                    batchingManager.enqueue(key, new Object());
                } catch (MaxPendingOperationExceededException e) {
                    fail("MaxPendingOperationExceededException thrown in blocking mode");
                }
            }
        }
    }

    @Test
    void testNonBlockingMode() throws InterruptedException, MaxPendingOperationExceededException {
        assertThrows(MaxPendingOperationExceededException.class, () -> {

            try (BatchingManager<Object, Object, String> batchingManager = createDumbBatchingManager(10, 1000, false, 1)) {
                String key = "key1";
                for (int i = 0; i < 100; i++) {
                    batchingManager.enqueue(key, new Object());
                }
            }
        });
    }

    private BatchingManager<Object, Object, String> createDumbBatchingManager(int maxPendingOperations,
                                                                              int batchTimeout,
                                                                              boolean blockingMode,
                                                                              int batchTaskSleepTime) {
        BatchingParameters batchingParameters = new BatchingParameters()
                .setMaxPendingOperations(maxPendingOperations)
                .setBatchTimeout(batchTimeout)
                .setBlockIfLimitExceeded(blockingMode);

        BatchTask<Object, Object, String> task = (a, b, c) -> Thread.sleep(batchTaskSleepTime);

        return new BatchingManager<>(new Object(), task, batchingParameters, "hBase-test-1", new NoopProfiler());
    }

}
