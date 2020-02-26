package ru.fix.stdlib.concurrency.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.DynamicPropertyListener;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Wrapper under runnable task. Save and check schedule value. If value was changed, task will be rescheduled.
 */
public class ReschedulableScheduler {
    private static final Logger log = LoggerFactory.getLogger(ReschedulableScheduler.class);

    private static final long DEFAULT_START_DELAY = 0;
    private static final String THREAD_POOL_SIZE_INDICATOR = "scheduled.tasks.count";

    private final ScheduledExecutorService executorService;

    private final Set<SelfSchedulableTaskWrapper> activeTasks;
    private final Profiler profiler;

    /**
     * ReschedulableScheduler based on {@link ProfiledScheduledThreadPoolExecutor} created with given parameters
     */
    public ReschedulableScheduler(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        this.executorService = new ProfiledScheduledThreadPoolExecutor(poolName, maxPoolSize, profiler);
        this.activeTasks = ConcurrentHashMap.newKeySet();
        this.profiler = profiler;
        profiler.attachIndicator(THREAD_POOL_SIZE_INDICATOR, () -> (long) activeTasks.size());
    }

    /**
     * change execution by schedule type
     *
     * @return result task from executionService
     */
    public ScheduledFuture<?> schedule(DynamicProperty<Schedule> scheduleSupplier,
                                       long startDelay,
                                       Runnable task) {

        SelfSchedulableTaskWrapper taskWrapper = new SelfSchedulableTaskWrapper(
                scheduleSupplier,
                startDelay,
                task,
                executorService,
                activeTasks::remove
        );

        activeTasks.add(taskWrapper);
        return taskWrapper.launch();
    }

    /**
     * change execution by schedule type with start delay 0
     *
     * @return result task from executionService
     */
    public ScheduledFuture<?> schedule(DynamicProperty<Schedule> schedule, Runnable task) {
        return schedule(schedule, DEFAULT_START_DELAY, task);
    }

    /**
     * shutdown scheduler's executorService
     * <p>
     * shorter version of {@code getExecutorService().shutdown()}
     */
    public void shutdown() {
        executorService.shutdown();
        cancelAllTasks(false);
        profiler.detachIndicator(THREAD_POOL_SIZE_INDICATOR);
    }

    /**
     * shutdownNow scheduler's executorService
     * <p>
     * shorter version of {@code getExecutorService().shutdownNow()}
     */
    public void shutdownNow() {
        executorService.shutdownNow();
        cancelAllTasks(true);
        profiler.detachIndicator(THREAD_POOL_SIZE_INDICATOR);
    }

    private void cancelAllTasks(boolean mayInterruptIfRunning) {
        synchronized (activeTasks) {
            activeTasks.forEach(taskWrapper -> taskWrapper.cancel(mayInterruptIfRunning));
        }
        while(!activeTasks.isEmpty()){
            synchronized (activeTasks) {
                activeTasks.iterator().next().cancel(mayInterruptIfRunning);
            }
        }
    }

    /**
     * Blocks until all tasks have completed execution after a shutdown
     * request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     */
    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return executorService.awaitTermination(timeout, timeUnit);
    }

    private static class SelfSchedulableTaskWrapper implements Runnable {

        private Schedule previousSchedule;
        private DynamicProperty<Schedule> schedule;
        private DynamicPropertyListener<Schedule> scheduleListener;
        private final long startDelay;

        private ScheduledFuture<?> scheduledFuture;

        private final Runnable task;
        private final ReschedulableSchedullerFuture reschedulableFuture =
                new ReschedulableSchedullerFuture(this);
        private Consumer<SelfSchedulableTaskWrapper> cancelHandler;

        private final ScheduledExecutorService executorService;

        private volatile ScheduleSettings settings;
        private volatile long lastExecutedTs = 0;
        private AtomicBoolean taskIsRunning = new AtomicBoolean(false);

        public SelfSchedulableTaskWrapper(DynamicProperty<Schedule> schedule,
                                          long startDelay,
                                          Runnable task,
                                          ScheduledExecutorService executorService,
                                          Consumer<SelfSchedulableTaskWrapper> cancelHandler) {
            this.schedule = schedule;
            this.startDelay = startDelay;
            this.task = task;
            this.executorService = executorService;
            this.cancelHandler = cancelHandler;
        }

        @Override
        @SuppressWarnings("squid:S1181")
        public void run() {
            //Preventing concurrent task launch for the case when
            // previously launched task still working,
            // schedule changed and triggered new scheduled task to launch
            if (!taskIsRunning.compareAndSet(false, true)) {
                log.trace("Preventing concurrent task launch; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                return;
            }

            ScheduleSettings currSettings = this.settings;
            if (currSettings.type == Schedule.Type.RATE) {
                //
                // If fixed rate tasks take more time that given rate
                // then next invocation of task starts to happen immediately
                // and total invocation rate will exceed rate limit
                //
                // Suppose standard java scheduled executor configured with fixed rate once in 10 sec.
                // If regular task takes 2 sec to execute then actual task execution rate will be as configured:
                // taskId, start time, end time:
                // 1: 0-2
                // 2: 10-12
                // 3: 20-22
                // 4: 30-32
                // 5: 40-42
                // 6: 50-52
                //
                // If first task will take more time, e.g. 33 seconds.
                // Then standard java schedule will remember how many scheduled task it didn't run.
                // And it will try to launch skipped tasks immediately as first opportunity occurred.
                // This will lead to wrong actual task launching rate:
                // taskId, start time, end time:
                // 1: 0-33
                // 2: 33-35
                // 3: 35-37
                // 4: 37-39
                // 5: 40-42
                // 6: 50-52
                // In this case tasks 2,3,4 are running with wrong rate.
                // To fix that we will skip all task invocations that occurred too earlie.
                //
                // skip wrong invocations
                long now = System.currentTimeMillis();
                if (now < lastExecutedTs + currSettings.periodValue - currSettings.safeDelay()) {
                    log.trace("skip wrong invocation; now={}, lastExecutedTs={}, currSettings={}; scheduledFuture={} with hash={}",
                            now, lastExecutedTs, currSettings,
                            scheduledFuture, System.identityHashCode(scheduledFuture));
                    return;
                }
                lastExecutedTs = now;
            }

            try {
                log.trace("running task; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                task.run();

            } catch (Throwable exc) {
                log.error("ReschedulableScheduler task failed due to: " + exc.getMessage(), exc);

            } finally {
                log.trace("Set taskIsRunning flag to false; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                taskIsRunning.compareAndSet(true, false);

                checkPreviousScheduleAndRestartTask(schedule.get());
            }
        }

        private synchronized void checkPreviousScheduleAndRestartTask(Schedule schedule) {
            if (reschedulableFuture.isCancelled()) {
                return;
            }

            if (!previousSchedule.equals(schedule)) {
                previousSchedule = schedule;
                log.trace("checkPreviousScheduleAndRestartTask cancelling  scheduledFuture {} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));

                this.scheduledFuture.cancel(false);
                this.scheduledFuture = schedule(this, schedule, this.startDelay);

                log.trace("checkPreviousScheduleAndRestartTask new scheduledFuture {} with hash={} is scheduled",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
            }
        }

        synchronized ScheduledFuture<?> launch() {
            this.scheduleListener = (oldVal, newVal) -> this.checkPreviousScheduleAndRestartTask(newVal);
            this.schedule.addListener(this.scheduleListener);
            Schedule schedule = this.schedule.get();

            this.scheduledFuture = schedule(this, schedule, this.startDelay);
            previousSchedule = schedule;

            log.trace("scheduledFuture={} with hash={} is launched",
                    scheduledFuture, System.identityHashCode(scheduledFuture));
            return reschedulableFuture;
        }

        private synchronized ScheduledFuture<?> schedule(SelfSchedulableTaskWrapper taskWrapper,
                                                         Schedule schedule, long startDelay) {
            long periodValue = schedule.getValue();
            Schedule.Type type = schedule.getType();

            settings = new ScheduleSettings(type, periodValue);
            switch (type) {
                case RATE:
                    return executorService.scheduleAtFixedRate(taskWrapper, startDelay,
                            periodValue, TimeUnit.MILLISECONDS);
                case DELAY:
                    return executorService.scheduleWithFixedDelay(taskWrapper, startDelay,
                            periodValue, TimeUnit.MILLISECONDS);
                default:
                    throw new IllegalArgumentException("Invalid schedule type: " + type);
            }

        }

        synchronized ScheduledFuture<?> getSchedullerFuture() {
            return scheduledFuture;
        }

        public synchronized void cancel(boolean mayInterruptIfRunning) {
            log.trace("cancelling scheduledFuture {} with hash={}",
                    scheduledFuture, System.identityHashCode(scheduledFuture));
            schedule.removeListener(this.scheduleListener);
            scheduledFuture.cancel(mayInterruptIfRunning);
            cancelHandler.accept(this);
        }
    }

    private static class ReschedulableSchedullerFuture implements ScheduledFuture<Object> {

        final SelfSchedulableTaskWrapper taskWrapper;

        public ReschedulableSchedullerFuture(SelfSchedulableTaskWrapper taskWrapper) {
            this.taskWrapper = taskWrapper;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return taskWrapper.getSchedullerFuture().getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return taskWrapper.getSchedullerFuture().compareTo(o);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            taskWrapper.cancel(mayInterruptIfRunning);
            return true;
        }

        @Override
        public boolean isCancelled() {
            return taskWrapper.getSchedullerFuture().isCancelled();
        }

        @Override
        public boolean isDone() {
            return taskWrapper.getSchedullerFuture().isDone();
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return taskWrapper.getSchedullerFuture().get();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return taskWrapper.getSchedullerFuture().get(timeout, unit);
        }
    }

    private static class ScheduleSettings {
        final Schedule.Type type;
        final long periodValue;

        public ScheduleSettings(Schedule.Type type, long periodValue) {
            this.type = type;
            this.periodValue = periodValue;
        }

        /**
         * safeDelay - possible safe delay between scheduled and real time task execution
         * due thread-scheduler limitations, so task will not rejected
         */
        long safeDelay() {

            long safeDelay = 15 * this.periodValue / 100;
            if (safeDelay < 1000) {
                safeDelay = 1000;
            } else if (safeDelay > 30_000) {
                safeDelay = 30_000;
            }
            return safeDelay;
        }
    }
}
