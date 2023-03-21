package ru.fix.stdlib.batching;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import ru.fix.aggregating.profiler.NoopProfiler;
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchingManagerHighContentionTest {

    private final static int TASK_COUNT = 200_000;
    private final static int BATCH = 200;

    /**
     * This test creates 50 BatchingManagers working in parallel.<br>
     * Every manager consumes and executes 200_000 tasks withing 20 batch threads.<br>
     * There are 5 unique keys for all tasks.
     * So every BatchingManagers works with 5 pending operations queues.
     * <p>
     * <b>Attention!</b>: log level trace strongly affects performance of this test!
     *
     * @param mode Block if batch is full and new operation have come
     */
    @ParameterizedTest
    @EnumSource(value = Mode.class)
    void highContentionTest(Mode mode) {

        BatchTask<Object, Object, String> task = (a, b, c) -> {
            synchronized (this) {
                this.wait(5);
            }
        };

        IntStream.range(1, 51).parallel().mapToObj(idx -> {

            ProfiledThreadPoolSettings poolSettings = ProfiledThreadPoolSettings
                    .defaultProfiledThreadPoolSettings(5, 20);

            BatchingParameters parameters = new BatchingParameters()
                    .setPoolSettings(poolSettings)
                    .setBatchTimeout(0)
                    .setBatchSize(BATCH)
                    .setBlockIfLimitExceeded(mode.blockIfLimitExceeded());

            return new BatchingManager<>(new Object(), task, parameters, "high-contention-" + idx, new NoopProfiler());
        }).forEach(manager -> {
            int keyIdx = 1;
            for (int i = 0; i < TASK_COUNT; ++i) {
                if (i % 5 == 0) {
                    keyIdx = 1;
                } else {
                    keyIdx++;
                }

                try {
                    manager.enqueue("key_" + keyIdx, new Object());
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

        assertTrue(true);
    }


    private enum Mode {
        BLOCKING_MODE,
        CONCURRENT_MODE;

        boolean blockIfLimitExceeded() {
            return BLOCKING_MODE == this;
        }
    }
}
