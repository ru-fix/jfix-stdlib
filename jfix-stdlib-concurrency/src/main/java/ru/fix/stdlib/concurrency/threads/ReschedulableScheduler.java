package ru.fix.stdlib.concurrency.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wrapper under runnable task. Save and check schedule value. If value was changed, task will be rescheduled.
 */
public class ReschedulableScheduler implements AutoCloseable {
    private static final long DEFAULT_START_DELAY = 0L;

    private final ScheduledExecutorService executorService;

    private final Set<SelfSchedulableTaskWrapper> activeTasks;
    private final Profiler profiler;
    private volatile boolean isShutdown = false;
    private final Logger log;
    private final String scheduledTasksIndicatorName;

    /**
     * ReschedulableScheduler based on {@link ProfiledScheduledThreadPoolExecutor} created with given parameters
     *
     * @param poolName will be used as
     */
    public ReschedulableScheduler(String poolName, DynamicProperty<Integer> maxPoolSize, Profiler profiler) {
        log = LoggerFactory.getLogger(ReschedulableScheduler.class.getName() + "." + poolName);
        this.executorService = new ProfiledScheduledThreadPoolExecutor(poolName, maxPoolSize, profiler);
        this.activeTasks = ConcurrentHashMap.newKeySet();
        this.profiler = profiler;
        scheduledTasksIndicatorName = "scheduled.pool." + poolName + ".tasks.count";
        profiler.attachIndicator(scheduledTasksIndicatorName, () -> (long) activeTasks.size());
    }

    private void detachIndicators() {
        profiler.detachIndicator(scheduledTasksIndicatorName);
    }

    /**
     * change execution by schedule type
     *
     * @return result task from executionService
     */
    public ScheduledFuture<?> schedule(DynamicProperty<Schedule> scheduleSupplier,
                                       DynamicProperty<Long> startDelay,
                                       Runnable task) {

        if (isShutdown) {
            throw new IllegalStateException("ReschedulableScheduler is shutdown and can not schedule new task." +
                    " Task: " + task);
        }

        SelfSchedulableTaskWrapper taskWrapper = new SelfSchedulableTaskWrapper(
                scheduleSupplier,
                startDelay,
                task,
                executorService,
                activeTasks::remove,
                log
        );

        activeTasks.add(taskWrapper);
        return taskWrapper.launch();
    }

    public ScheduledFuture<?> schedule(DynamicProperty<Schedule> scheduleSupplier, long startDelay, Runnable task) {
        return schedule(scheduleSupplier, DynamicProperty.of(startDelay), task);
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
        cancelAllTasks(false);
        executorService.shutdown();
        detachIndicators();
        isShutdown = true;
    }

    /**
     * shutdownNow scheduler's executorService
     * <p>
     * shorter version of {@code getExecutorService().shutdownNow()}
     */
    public void shutdownNow() {
        cancelAllTasks(true);
        executorService.shutdownNow();
        detachIndicators();
        isShutdown = true;
    }

    private void cancelAllTasks(boolean mayInterruptIfRunning) {
        for (SelfSchedulableTaskWrapper task : activeTasks.toArray(new SelfSchedulableTaskWrapper[0])) {
            task.cancel(mayInterruptIfRunning);
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

        private final Logger log;

        private ScheduleSettingsSnapshot prevScheduleSettingsSnapshot;
        private final DynamicProperty<Schedule> schedule;
        private PropertySubscription<Schedule> scheduleSubscription;
        private PropertySubscription<Long> initialDelaySubscription;
        private final DynamicProperty<Long> startDelay;

        private ScheduledFuture<?> scheduledFuture;

        private final Runnable task;
        private final ReschedulableSchedullerFuture reschedulableFuture =
                new ReschedulableSchedullerFuture(this);

        private final Consumer<SelfSchedulableTaskWrapper> cancelHandler;

        private final ScheduledExecutorService executorService;

        private volatile ScheduleSettings settings;
        private volatile long lastExecutedTs = 0L;
        private final AtomicBoolean taskIsRunning = new AtomicBoolean(false);

        public SelfSchedulableTaskWrapper(DynamicProperty<Schedule> schedule,
                                          DynamicProperty<Long> startDelay,
                                          Runnable task,
                                          ScheduledExecutorService executorService,
                                          Consumer<SelfSchedulableTaskWrapper> cancelHandler,
                                          Logger log) {
            this.schedule = schedule;
            this.startDelay = startDelay;
            this.task = task;
            this.executorService = executorService;
            this.cancelHandler = cancelHandler;
            this.log = log;
        }

        @Override
        @SuppressWarnings("squid:S1181")
        public void run() {
            ScheduledFuture<?> scheduledFuture;
            synchronized (this) {
                scheduledFuture = this.scheduledFuture;
            }


            //Preventing concurrent task launch for the case when
            // previously launched task still working,
            // schedule changed and triggered new scheduled task to launch
            if (!taskIsRunning.compareAndSet(false, true)) {
                log.trace("Preventing concurrent task launch; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                return;
            }

            try {

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

                log.trace("running task; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                task.run();

            } catch (Throwable exc) {
                log.error("ReschedulableScheduler task failed due to: " + exc.getMessage(), exc);

            } finally {
                log.trace("Set taskIsRunning flag to false; scheduledFuture={} with hash={}",
                        scheduledFuture, System.identityHashCode(scheduledFuture));
                taskIsRunning.compareAndSet(true, false);

                checkPreviousScheduleAndRestartTask(new ScheduleSettingsSnapshot(schedule.get(), startDelay.get()));
            }
        }

        private synchronized void checkPreviousScheduleAndRestartTask(ScheduleSettingsSnapshot scheduleSettingsSnapshot) {
            ScheduledFuture<?> curScheduledFuture = scheduledFuture;
            if (curScheduledFuture != null && curScheduledFuture.isCancelled()) {
                return;
            }

            ScheduleSettingsSnapshot settingsSnapshot = prevScheduleSettingsSnapshot;
            if (settingsSnapshot != null && settingsSnapshot.equals(scheduleSettingsSnapshot)) {
                return;
            }

            this.prevScheduleSettingsSnapshot = scheduleSettingsSnapshot;

            if (curScheduledFuture != null) {
                log.trace("checkPreviousScheduleAndRestartTask cancelling  scheduledFuture {} with hash={}",
                        curScheduledFuture, System.identityHashCode(curScheduledFuture));
                curScheduledFuture.cancel(false);
            }

            this.scheduledFuture = schedule(
                    this,
                    scheduleSettingsSnapshot.schedule,
                    scheduleSettingsSnapshot.startDelay
            );

            log.trace("checkPreviousScheduleAndRestartTask new scheduledFuture {} with hash={} is scheduled",
                    curScheduledFuture, System.identityHashCode(curScheduledFuture));

        }

        public synchronized ScheduledFuture<?> launch() {

            this.scheduleSubscription = this.schedule
                    .createSubscription()
                    .setAndCallListener((oldVal, newVal) ->
                            checkPreviousScheduleAndRestartTask(new ScheduleSettingsSnapshot(newVal, startDelay.get()))
                    );

            this.initialDelaySubscription = startDelay
                    .createSubscription()
                    .setAndCallListener((oldValue, newValue) ->
                            checkPreviousScheduleAndRestartTask(new ScheduleSettingsSnapshot(schedule.get(), newValue))
                    );

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

        synchronized <T> T accessScheduledFuture(Function<ScheduledFuture<?>, T> accessor) {
            return accessor.apply(scheduledFuture);
        }

        public synchronized void cancel(boolean mayInterruptIfRunning) {
            log.trace("cancelling scheduledFuture {} with hash={}",
                    scheduledFuture, System.identityHashCode(scheduledFuture));
            scheduleSubscription.close();
            initialDelaySubscription.close();
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
            return taskWrapper.accessScheduledFuture(future -> future.getDelay(unit));
        }

        @Override
        public int compareTo(Delayed o) {
            return taskWrapper.accessScheduledFuture(future -> future.compareTo(o));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            taskWrapper.cancel(mayInterruptIfRunning);
            return true;
        }

        @Override
        public boolean isCancelled() {
            return taskWrapper.accessScheduledFuture(future -> future.isCancelled());
        }

        @Override
        public boolean isDone() {
            return taskWrapper.accessScheduledFuture(future -> future.isDone());
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return taskWrapper.accessScheduledFuture(Function.identity()).get();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return taskWrapper.accessScheduledFuture(Function.identity()).get(timeout, unit);
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

        @Override
        public String toString() {
            return "ScheduleSettings{" +
                    "type=" + type +
                    ", periodValue=" + periodValue +
                    '}';
        }
    }

    /**
     * Same as {@link #shutdown()}
     */
    @Override
    public void close() {
        shutdown();
    }

    private static class ScheduleSettingsSnapshot {

        final Schedule schedule;
        final long startDelay;

        public ScheduleSettingsSnapshot(
                Schedule schedule,
                long startDelay
        ) {
            this.schedule = schedule;
            this.startDelay = startDelay;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScheduleSettingsSnapshot that = (ScheduleSettingsSnapshot) o;
            return startDelay == that.startDelay &&
                    Objects.equals(schedule, that.schedule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedule, startDelay);
        }
    }
}
