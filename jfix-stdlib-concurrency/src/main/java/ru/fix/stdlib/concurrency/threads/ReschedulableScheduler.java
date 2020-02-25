package ru.fix.stdlib.concurrency.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper under runnable task. Save and check schedule value. If value was changed, task will be rescheduled.
 */
public class ReschedulableScheduler {
    private static final Logger log = LoggerFactory.getLogger(ReschedulableScheduler.class);

    private static final long DEFAULT_START_DELAY = 0;

    private final ScheduledExecutorService executorService;

    /**
     * ReschedulableScheduler based on given executorService
     * It would be better to use {@link ProfiledScheduledThreadPoolExecutor} as executorService
     */
    public ReschedulableScheduler(ScheduledExecutorService executorService) {
        this.executorService = executorService;
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
                executorService
        );
        return taskWrapper.launch();
    }

    /**
     * change execution by schedule type with start delay 0
     *
     * @return result task from executionService
     */
    public ScheduledFuture<?> schedule(DynamicProperty<Schedule> schedule, Runnable task) {
        SelfSchedulableTaskWrapper taskWrapper = new SelfSchedulableTaskWrapper(
                schedule,
                DEFAULT_START_DELAY,
                task,
                executorService
        );
        return taskWrapper.launch();
    }

    /**
     * shutdown scheduler's executorService
     * <p>
     * shorter version of {@code getExecutorService().shutdown()}
     */
    public void shutdown() {
        executorService.shutdown();
    }

    /**
     * shutdownNow scheduler's executorService
     * <p>
     * shorter version of {@code getExecutorService().shutdownNow()}
     */
    public void shutdownNow() {
        executorService.shutdownNow();
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
        private final long startDelay;

        private ScheduledFuture<?> scheduledFuture;

        private final Runnable task;
        private final ReschedulableSchedullerFuture reschedulableFuture =
                new ReschedulableSchedullerFuture(this);

        private final ScheduledExecutorService executorService;

        private volatile ScheduleSettings settings;
        private volatile long lastExecutedTs = 0;
        private AtomicBoolean taskIsRunning = new AtomicBoolean(false);

        public SelfSchedulableTaskWrapper(DynamicProperty<Schedule> schedule,
                                          long startDelay,
                                          Runnable task,
                                          ScheduledExecutorService executorService) {
            this.schedule = schedule;
            this.schedule.addListener((oldVal, newVal) -> this.checkPreviousScheduleAndRestartTask(newVal));
            this.startDelay = startDelay;
            this.task = task;
            this.executorService = executorService;
        }

        @Override
        @SuppressWarnings("squid:S1181")
        public void run() {
            //Preventing concurrent task launch for the case when
            // previously launched task still working,
            // schedule changed and triggered new scheduled task to launch
            if(!taskIsRunning.compareAndSet(false, true)){
                log.trace("Preventing concurrent task launch; scheduledFuture={}", scheduledFuture);
                return;
            }
            log.trace("Set taskIsRunning true; scheduledFuture={}", scheduledFuture);


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
                    return;
                }
                lastExecutedTs = now;
            }

            try {
                task.run();

            } catch (Throwable exc) {
                log.error("ReschedulableScheduler task failed due to: " + exc.getMessage(), exc);

            } finally {
                log.trace("Set taskIsRunning false; scheduledFuture={}", scheduledFuture);
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
                this.scheduledFuture.cancel(false);
                this.scheduledFuture = schedule(this, schedule, this.startDelay);
            }
        }

        synchronized ScheduledFuture<?> launch() {
            Schedule schedule = this.schedule.get();

            this.scheduledFuture = schedule(this, schedule, this.startDelay);
            previousSchedule = schedule;

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
            scheduledFuture.cancel(mayInterruptIfRunning);
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
