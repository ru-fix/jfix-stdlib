package ru.fix.stdlib.concurrency.threads;

import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProfiledThreadPoolExecutor extends ThreadPoolExecutor {

    private static final long THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC = 60;

    final Profiler profiler;

    final ThreadLocal<ProfiledCall> runExecution = new ThreadLocal<>();

    private abstract class ProfiledRunnable implements Runnable {
        private final String poolName;

        public ProfiledRunnable(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + poolName + ")";
        }
    }

    /**
     * Invoked by ctor
     */
    private static ThreadFactory threadFactory(String poolName) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> new Thread(runnable, poolName + "-" + counter.getAndIncrement());
    }

    private final String poolName;
    private final String queueIndicatorName;
    private final String activeThreadsIndicatorName;
    private final String poolSizeIndicatorName;
    private final String callAwaitName;
    private final String callRunName;


    public ProfiledThreadPoolExecutor(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        super(
                maxPoolSize.get(),
                maxPoolSize.get(),
                THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory(poolName)
        );
        this.poolName = poolName;
        this.profiler = profiler;

        String profilerPoolName = poolName.replace('.', '_');

        queueIndicatorName = "pool." + profilerPoolName + ".queue";
        activeThreadsIndicatorName = "pool." + profilerPoolName + ".activeThreads";
        callAwaitName = "pool." + profilerPoolName + ".await";
        callRunName = "pool." + profilerPoolName + ".run";
        poolSizeIndicatorName = "pool." + profilerPoolName + ".poolSize";

        super.allowCoreThreadTimeOut(true);

        maxPoolSize.addListener(this::setMaxPoolSize);

        profiler.attachIndicator(queueIndicatorName, () -> (long) this.getQueue().size());
        profiler.attachIndicator(activeThreadsIndicatorName, () -> (long) this.getActiveCount());
        profiler.attachIndicator(poolSizeIndicatorName, () -> (long) this.getPoolSize());
    }

    @Override
    public void execute(Runnable command) {
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
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.setCorePoolSize(maxPoolSize);
        this.setMaximumPoolSize(maxPoolSize);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.poolName + ")";
    }
}
