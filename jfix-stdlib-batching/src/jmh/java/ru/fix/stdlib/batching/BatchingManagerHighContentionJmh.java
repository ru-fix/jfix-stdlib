package ru.fix.stdlib.batching;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import ru.fix.aggregating.profiler.NoopProfiler;

import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * This test compares 2 cases of BatchProcessor threads tracking
 * <dl>
 *
 * <dt><b>tracking via semaphore acquire/release</b></dt>
 * <dd>{@link BatchingManagerThreadSemaphore}
 * BatchingManagerThread tracks for free threads in batchProcessorPool using semaphore.
 * Semaphore permits count equals to batchProcessorPool size. If all batchProcessorPool threads are busy,
 * semaphore will have no permits and BatchingManagerThread will wait for some free thread in batchProcessorPool<p>
 * This implementations depends on batchProcessorPool size. BatchProcessorPool size can be changed in runtime,
 * but Semaphore permits <b>could not be changed in runtime</b> and such situation may case problems.<p>
 * This implementation can be found in {@code src/jmh/java} and <b>it is not available in production code!</b>
 * {@link BatchingManagerSemaphore},
 * {@link BatchingManagerThreadSemaphore},
 * {@link BatchProcessorSemaphore}
 * </dd>
 *
 * <dt><b>tracking via object wait/notify</b></dt>
 * <dd>{@link BatchingManagerThread}
 * BatchingManagerThread tracks for free threads in batchProcessorPool using wait/notify synchronization.
 * This implementations does not depend on batchProcessorPool size. Its performance practically the same as semaphore.
 * </dd>
 *
 * </dl>
 */
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
