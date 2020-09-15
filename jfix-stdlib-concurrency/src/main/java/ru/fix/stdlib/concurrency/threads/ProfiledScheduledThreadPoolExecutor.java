package ru.fix.stdlib.concurrency.threads;

import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProfiledScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    private static final long THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC = 60;

    final Profiler profiler;

    final ThreadLocal<ProfiledCall> runExecution = new ThreadLocal<>();
    private final PropertySubscription<Integer> maxPoolSizeSubscription;

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

    private final String poolName;
    private final String queueIndicatorName;
    private final String activeThreadsIndicatorName;
    private final String callRunName;
    private final String poolSizeIndicatorName;


    public ProfiledScheduledThreadPoolExecutor(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        super(
                maxPoolSize.get()
        );
        setThreadFactory(threadFactory(poolName));
        this.profiler = profiler;
        this.poolName = poolName;

        String profilerPoolName = poolName.replace('.', '_');

        queueIndicatorName = "pool." + profilerPoolName + ".queue";
        activeThreadsIndicatorName = "pool." + profilerPoolName + ".activeThreads";
        callRunName = "pool." + profilerPoolName + ".run";
        poolSizeIndicatorName = "pool." + profilerPoolName + ".poolSize";

        this.setRemoveOnCancelPolicy(true);
        this.setKeepAliveTime(THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC, TimeUnit.SECONDS);
        this.allowCoreThreadTimeOut(true);

        this.maxPoolSizeSubscription = maxPoolSize
                .createSubscription()
                .setAndCallListener((oldVal, newVal) -> this.setMaxPoolSize(newVal));

        profiler.attachIndicator(queueIndicatorName, () -> (long) this.getQueue().size());
        profiler.attachIndicator(activeThreadsIndicatorName, () -> (long) this.getActiveCount());
        profiler.attachIndicator(poolSizeIndicatorName, () -> (long) this.getPoolSize());
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        runExecution.set(profiler.profiledCall(callRunName).start());
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        ProfiledCall runCall = runExecution.get();
        if (runCall != null) {
            runCall.stop();
            runExecution.remove();
        }
        super.afterExecute(r, t);
    }

    @Override
    protected void terminated() {
        profiler.detachIndicator(queueIndicatorName);
        profiler.detachIndicator(activeThreadsIndicatorName);
        profiler.detachIndicator(poolSizeIndicatorName);
        maxPoolSizeSubscription.close();
        super.terminated();
    }


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
}
