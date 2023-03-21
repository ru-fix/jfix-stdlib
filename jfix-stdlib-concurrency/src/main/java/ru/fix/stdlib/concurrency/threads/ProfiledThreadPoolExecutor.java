package ru.fix.stdlib.concurrency.threads;

import org.jetbrains.annotations.NotNull;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings;
import ru.fix.stdlib.concurrency.settings.Subscriptions;
import ru.fix.stdlib.concurrency.settings.factory.PoolSettingsSubscriptionFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProfiledThreadPoolExecutor extends ThreadPoolExecutor {

    private final Profiler profiler;

    private final ThreadLocal<ProfiledCall> runExecution = new ThreadLocal<>();
    private final PropertySubscription<ProfiledThreadPoolSettings> poolSettingsSubscription;

    private final String poolName;
    private final String queueIndicatorName;
    private final String activeThreadsIndicatorName;
    private final String poolSizeIndicatorName;
    private final String maxPoolSizeIndicatorName;
    private final String callAwaitName;
    private final String callRunName;

    public ProfiledThreadPoolExecutor(
            DynamicProperty<ProfiledThreadPoolSettings> poolSettings,
            String poolName,
            Profiler profiler
    ) {
        this(poolSettings,
                poolName,
                profiler,
                Subscriptions.getDefaultPoolSettingsSubscriptionFactory()
        );
    }

    /**
     * Creates instance of ProfiledThreadPoolExecutor with specified parameters:
     * <li> corePoolSize = maximumPoolSize
     * <li> keepAliveTime = 60 seconds, see {@link ProfiledThreadPoolSettings#THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION}
     * <li> allowCoreThreadTimeOut = true
     <p>
     * @deprecated <b>should be removed in future releases</b>
     * @param poolName name of the pool
     * @param maxPoolSize maximum allowed number of threads. This parameter makes corePoolSize = maximumPoolSize!!!
     * @param profiler instance of AggregatingProfiler
     * @see ProfiledThreadPoolExecutor#deprecatedPoolSettings(DynamicProperty)
     */
    @Deprecated
    public ProfiledThreadPoolExecutor(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        this(deprecatedPoolSettings(maxPoolSize),
                poolName,
                profiler,
                Subscriptions.getDeprecatedPoolSettingsSubscriptionFactory()
        );
    }

    private ProfiledThreadPoolExecutor(
            DynamicProperty<ProfiledThreadPoolSettings> poolSettings,
            String poolName,
            Profiler profiler,
            PoolSettingsSubscriptionFactory poolSettingsSubscriptionFactory
    ) {
        super(
                poolSettings.get().getCorePoolSize(),
                poolSettings.get().getMaxPoolSize(),
                poolSettings.get().getKeepAliveTime().getSeconds(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );

        this.poolName = poolName;

        setThreadFactory(threadFactory(this.poolName));
        allowCoreThreadTimeOut(poolSettings.get().getAllowCoreThreadTimeout());

        String profilerPoolName = this.poolName.replace('.', '_');

        this.profiler = profiler;

        queueIndicatorName = metricName(profilerPoolName, "queue");
        activeThreadsIndicatorName = metricName(profilerPoolName, "activeThreads");
        callAwaitName = metricName(profilerPoolName, "await");
        callRunName = metricName(profilerPoolName, "run");
        poolSizeIndicatorName = metricName(profilerPoolName, "poolSize");
        maxPoolSizeIndicatorName = metricName(profilerPoolName, "maxPoolSize");

        poolSettingsSubscription = poolSettingsSubscriptionFactory.apply(this, poolSettings);

        this.profiler.attachIndicator(queueIndicatorName, () -> (long) getQueue().size());
        this.profiler.attachIndicator(activeThreadsIndicatorName, () -> (long) getActiveCount());
        this.profiler.attachIndicator(poolSizeIndicatorName, () -> (long) getPoolSize());
        this.profiler.attachIndicator(maxPoolSizeIndicatorName, () -> (long) getMaximumPoolSize());
    }

    @Override
    public void execute(@NotNull Runnable command) {
        ProfiledCall awaitCall = profiler.profiledCall(callAwaitName).start();

        super.execute(new ProfiledRunnable(poolName) {
            @Override
            public void run() {
                awaitCall.stop();
                command.run();
            }
        });
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        runExecution.set(profiler.profiledCall(callRunName).start());
        super.beforeExecute(thread, runnable);
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable thread) {
        ProfiledCall runCall = runExecution.get();
        if (runCall != null) {
            runCall.stop();
            runExecution.remove();
        }
        super.afterExecute(runnable, thread);
    }

    @Override
    protected void terminated() {
        profiler.detachIndicator(queueIndicatorName);
        profiler.detachIndicator(activeThreadsIndicatorName);
        profiler.detachIndicator(poolSizeIndicatorName);
        profiler.detachIndicator(maxPoolSizeIndicatorName);
        poolSettingsSubscription.close();
        super.terminated();
    }

    /**
     * @deprecated <b>should be removed in future releases</b>
     * @param maxPoolSize {@link ThreadPoolExecutor#setMaximumPoolSize(int)}
     */
    @Deprecated
    public void setMaxPoolSize(int maxPoolSize) {
        if (maxPoolSize >= getMaximumPoolSize()) {
            setMaximumPoolSize(maxPoolSize);
            setCorePoolSize(maxPoolSize);
        } else {
            setCorePoolSize(maxPoolSize);
            setMaximumPoolSize(maxPoolSize);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.poolName + ")";
    }

    /**
     * Invoked by ctor
     */
    private ThreadFactory threadFactory(String poolName) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, poolName + "-" + counter.getAndIncrement());
            thread.setContextClassLoader(getClass().getClassLoader());
            return thread;
        };
    }

    private String metricName(String profilerPoolName, String metricName) {
        return "pool." + profilerPoolName + "." + metricName;
    }

    private static DynamicProperty<ProfiledThreadPoolSettings> deprecatedPoolSettings(
            DynamicProperty<Integer> maxPoolSize
    ) {
        return maxPoolSize.map(maxSize -> new ProfiledThreadPoolSettings(
                maxSize,
                maxSize,
                ProfiledThreadPoolSettings.THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION,
                true
        ));
    }

    private abstract static class ProfiledRunnable implements Runnable {
        private final String poolName;

        public ProfiledRunnable(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + poolName + ")";
        }
    }
}
