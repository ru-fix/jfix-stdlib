package ru.fix.stdlib.batching;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import ru.fix.aggregating.profiler.NoopProfiler;

import java.util.function.IntConsumer;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
public class BatchingManagerHighContentionJmh {

    private static final int TASKS = 100_000;
    private static final int BATCH = 200;

    private final BatchTask<Object, Object, String> task = (a, b, c) -> Thread.sleep(5);

    @Benchmark
    public void semaphoreTest() throws InterruptedException {
        batchingManagersParallelStream().mapToObj(idx -> new BatchingManagerSemaphore<>(
                new Object(),
                task,
                newBatchingParameters(),
                "semaphore-" + idx,
                new NoopProfiler())
        ).forEach(manager -> new BatchingManagerEnqueuer().enqueueAllTasks(keyIdx -> {
            try {
                manager.enqueue("key_" + keyIdx, new Object());
            } catch (InterruptedException ex) {
                //nop
            }
        }));
    }

    @Benchmark
    public void poolQueueSizeTest() throws InterruptedException {
        batchingManagersParallelStream().mapToObj(idx -> new BatchingManager<>(
                new Object(),
                task,
                newBatchingParameters(),
                "pool-queue-size-" + idx,
                new NoopProfiler())
        ).forEach(manager -> new BatchingManagerEnqueuer().enqueueAllTasks(keyIdx -> {
            try {
                manager.enqueue("key_" + keyIdx, new Object());
            } catch (InterruptedException ex) {
                //nop
            }
        }));
    }

    private static IntStream batchingManagersParallelStream() {
        return IntStream.range(1, 3).parallel();
    }

    private static BatchingParameters newBatchingParameters() {
        return new BatchingParameters()
                .setBatchTimeout(0)
                .setBatchThreads(6)
                .setBatchSize(BATCH)
                .setMaxPendingOperations(1000_000)
                .setBlockIfLimitExceeded(false);
    }

    private static class BatchingManagerEnqueuer {

        public void enqueueAllTasks(IntConsumer taskEnqueuer) {
            int keyIdx = 1;
            for (int i = 0; i < TASKS; ++i) {
                if (i % 5 == 0) {
                    keyIdx = 1;
                } else {
                    keyIdx++;
                }

                taskEnqueuer.accept(keyIdx);
            }
        }
    }
}
