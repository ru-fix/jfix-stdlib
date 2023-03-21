package ru.fix.stdlib.concurrency.threads;

import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings;

import java.util.concurrent.ForkJoinPool;

/**
 * @author Kamil Asfandiyarov
 */
public final class NamedExecutors {

    /**
     * Create new thread pool with dynamic size
     * Usage example:
     * <pre>{@code
     *  ExecutorService executor = NamedExecutors.newDynamicPool(
     *      "dao-thread-pool",
     *      poolSize,
     *      profiler
     *  );
     * }
     * </pre>
     * @deprecated <b>should be removed in future releases</b>
     * @see NamedExecutors#newDynamicPool(String, DynamicProperty, Profiler)
     */
    @Deprecated
    public static ProfiledThreadPoolExecutor newDynamicPool(String poolName,
                                                            DynamicProperty<Integer> maxPoolSize,
                                                            Profiler profiler) {

        return new ProfiledThreadPoolExecutor(poolName, maxPoolSize, profiler);
    }

    /**
     * Creates new thread pool with dynamic size
     */
    public static ProfiledThreadPoolExecutor newDynamicPool(
            DynamicProperty<ProfiledThreadPoolSettings> poolSettings,
            String poolName,
            Profiler profiler
    ) {
        return new ProfiledThreadPoolExecutor(poolSettings, poolName, profiler);
    }

    public static ProfiledThreadPoolExecutor newSingleThreadPool(String poolName,
                                                                 Profiler profiler) {

        return new ProfiledThreadPoolExecutor(
                DynamicProperty.of(ProfiledThreadPoolSettings.defaultSingleThreadPoolSettings()),
                poolName,
                profiler
        );
    }

    public static ReschedulableScheduler newScheduler(String poolName,
                                                      DynamicProperty<Integer> maxPoolSize,
                                                      Profiler profiler) {
        return new ReschedulableScheduler(
                poolName,
                maxPoolSize,
                profiler
        );
    }

    public static ReschedulableScheduler newSingleThreadScheduler(String poolName,
                                                                  Profiler profiler) {
        return new ReschedulableScheduler(
                poolName,
                DynamicProperty.of(1),
                profiler
        );
    }

    public static ProfiledScheduledThreadPoolExecutor newScheduledExecutor(String poolName,
                                                                           DynamicProperty<Integer> maxPoolSize,
                                                                           Profiler profiler) {
        return new ProfiledScheduledThreadPoolExecutor(
                poolName,
                maxPoolSize,
                profiler
        );
    }

    public static ProfiledScheduledThreadPoolExecutor newSingleThreadScheduledExecutor(String poolName,
                                                                                       Profiler profiler) {
        return new ProfiledScheduledThreadPoolExecutor(
                poolName,
                DynamicProperty.of(1),
                profiler
        );
    }

    public static void profileCommonPool(Profiler profiler) {

        profiler.attachIndicator("pool.commonPool.poolSize",
                () -> (long) ForkJoinPool.commonPool().getPoolSize());

        profiler.attachIndicator("pool.commonPool.activeThread",
                () -> (long) ForkJoinPool.commonPool().getActiveThreadCount());

        profiler.attachIndicator("pool.commonPool.runningThread",
                () -> (long) ForkJoinPool.commonPool().getRunningThreadCount());

        profiler.attachIndicator("pool.commonPool.queue",
                () -> (long) ForkJoinPool.commonPool().getQueuedSubmissionCount());

        profiler.attachIndicator("pool.commonPool.steal",
                () -> ForkJoinPool.commonPool().getStealCount());

    }


    private NamedExecutors() {

    }
}
