package ru.fix.stdlib.concurrency.threads;

import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For complete documentation details see {@link ThreadPoolExecutor}
 * <dl>
 *
 * <dt><b>Core and maximum pool sizes</b></dt>
 * <dd>When a new task is submitted in method {@link #execute(Runnable)},
 * if fewer than corePoolSize threads are running, a new thread is
 * created to handle the request, even if other worker threads are
 * idle.  Else if fewer than maximumPoolSize threads are running, a
 * new thread will be created to handle the request only if the queue
 * is full. By setting corePoolSize and maximumPoolSize the same, you
 * create a fixed-size thread pool. By setting maximumPoolSize to an
 * essentially unbounded value such as {@code Integer.MAX_VALUE}, you
 * allow the pool to accommodate an arbitrary number of concurrent
 * tasks. Most typically, core and maximum pool sizes are set only
 * upon construction, but they may also be changed dynamically using
 * {@link #setCorePoolSize} and {@link #setMaximumPoolSize}. </dd>
 *
 * <dt><b>On-demand construction</b></dt>
 * <dd>By default, even core threads are initially created and
 * started only when new tasks arrive, but this can be overridden
 * dynamically using method {@link #prestartCoreThread} or {@link
 * #prestartAllCoreThreads}.  You probably want to prestart threads if
 * you construct the pool with a non-empty queue. </dd>
 *
 * <dt><b>Keep-alive times</b></dt>
 * <dd>If the pool currently has more than corePoolSize threads,
 * excess threads will be terminated if they have been idle for more
 * than the keepAliveTime (see {@link #getKeepAliveTime(TimeUnit)}).
 * This provides a means of reducing resource consumption when the
 * pool is not being actively used. If the pool becomes more active
 * later, new threads will be constructed. This parameter can also be
 * changed dynamically using method {@link #setKeepAliveTime(long,
 * TimeUnit)}.  Using a value of {@code Long.MAX_VALUE} {@link
 * TimeUnit#NANOSECONDS} effectively disables idle threads from ever
 * terminating prior to shut down. By default, the keep-alive policy
 * applies only when there are more than corePoolSize threads, but
 * method {@link #allowCoreThreadTimeOut(boolean)} can be used to
 * apply this time-out policy to core threads as well, so long as the
 * keepAliveTime value is non-zero. </dd>
 *
 * <dt><b>Queuing</b></dt>
 * <dd>Any {@link BlockingQueue} may be used to transfer and hold
 * submitted tasks.  The use of this queue interacts with pool sizing:
 * <ul>
 * <li>If fewer than corePoolSize threads are running, the Executor
 * always prefers adding a new thread
 * rather than queuing.
 * <li>If corePoolSize or more threads are running, the Executor
 * always prefers queuing a request rather than adding a new
 * thread.
 * <li>If a request cannot be queued, a new thread is created unless
 * this would exceed maximumPoolSize, in which case, the task will be
 * rejected.
 * </ul>
 * </dd>
 *
 * </dl>
 *
 * Developers are urged to use the more convenient
 * {@link Executors} factory methods {@link Executors#newCachedThreadPool}
 * (unbounded thread pool, with automatic thread reclamation),
 * {@link Executors#newFixedThreadPool} (fixed size thread pool)
 * and {@link Executors#newSingleThreadExecutor} (single background thread),
 * that preconfigure settings for the most common usage scenarios.
 */
public class ProfiledThreadPoolExecutor extends ThreadPoolExecutor {

    private static final long THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC = 60;

    private final Profiler profiler;

    private final ThreadLocal<ProfiledCall> runExecution = new ThreadLocal<>();
    private final PropertySubscription<Integer> maxPoolSizeSubscription;

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
    private final String poolSizeIndicatorName;
    private final String maxPoolSizeIndicatorName;
    private final String callAwaitName;
    private final String callRunName;


    public ProfiledThreadPoolExecutor(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        super(
                maxPoolSize.get(),
                maxPoolSize.get(),
                THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION_SEC, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
        setThreadFactory(threadFactory(poolName));
        this.poolName = poolName;
        this.profiler = profiler;

        String profilerPoolName = poolName.replace('.', '_');

        queueIndicatorName = metricName(profilerPoolName, "queue");
        activeThreadsIndicatorName = metricName(profilerPoolName, "activeThreads");
        callAwaitName = metricName(profilerPoolName, "await");
        callRunName = metricName(profilerPoolName, "run");
        poolSizeIndicatorName = metricName(profilerPoolName, "poolSize");
        maxPoolSizeIndicatorName = metricName(profilerPoolName, "maxPoolSize");

        super.allowCoreThreadTimeOut(true);

        this.maxPoolSizeSubscription = maxPoolSize
                .createSubscription()
                .setAndCallListener((oldVal, newVal) -> this.setMaxPoolSize(newVal));

        profiler.attachIndicator(queueIndicatorName, () -> (long) this.getQueue().size());
        profiler.attachIndicator(activeThreadsIndicatorName, () -> (long) this.getActiveCount());
        profiler.attachIndicator(poolSizeIndicatorName, () -> (long) this.getPoolSize());
        profiler.attachIndicator(maxPoolSizeIndicatorName, () -> (long) this.getMaximumPoolSize());
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
        profiler.detachIndicator(maxPoolSizeIndicatorName);
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

    private String metricName(String profilerPoolName, String metricName) {
        return "pool." + profilerPoolName + "." + metricName;
    }
}
