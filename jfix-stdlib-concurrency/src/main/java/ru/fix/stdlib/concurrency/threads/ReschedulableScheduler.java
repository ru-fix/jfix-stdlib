package ru.fix.stdlib.concurrency.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.concurrent.*;

/**
 * Wrapper under runnable task. Save and check schedule value. If value was changed, task will be rescheduled.
 */
public class ReschedulableScheduler {
    private static final Logger log = LoggerFactory.getLogger(ReschedulableScheduler.class);

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

        SelfSchedulableTaskWrapper taskWrapper = new SelfSchedulableTaskWrapper(scheduleSupplier, task,
                executorService);
        return taskWrapper.launch(startDelay);
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
        private DynamicProperty<Schedule> scheduleSupplier;

        private ScheduledFuture<?> scheduledFuture;

        private final Runnable task;
        private final ReschedulableSchedullerFuture reschedulableFuture =
                new ReschedulableSchedullerFuture(this);

        private final ScheduledExecutorService executorService;

        private volatile ScheduleSettings settings;
        private volatile long lastExecutedTs = 0;

        public SelfSchedulableTaskWrapper(DynamicProperty<Schedule> scheduleSupplier,
                                          Runnable task, ScheduledExecutorService executorService) {
            this.scheduleSupplier = scheduleSupplier;
            this.task = task;
            this.executorService = executorService;
        }

        @Override
        @SuppressWarnings("squid:S1181")
        public void run() {
            ScheduleSettings currSettings = this.settings;
            if (currSettings.type == Schedule.Type.RATE) {
                //If fixed rate tasks take more time that given rate
                //then next invocation of task starts to happen immediately
                //and total invocation rate will exceed rate limit
                //skip such invocations
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
                checkPreviousScheduleAndRestartTask();
            }
        }

        private synchronized void checkPreviousScheduleAndRestartTask() {
            Schedule schedule = scheduleSupplier.get();

            if (reschedulableFuture.isCancelled()) {
                return;
            }

            if (!previousSchedule.equals(schedule)) {
                previousSchedule = schedule;
                this.scheduledFuture.cancel(false);
                this.scheduledFuture = schedule(this, schedule, schedule.getValue());
            }
        }


        synchronized ScheduledFuture<?> launch(long startDelay) {
            Schedule schedule = scheduleSupplier.get();

            this.scheduledFuture = schedule(this, schedule, startDelay);
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
