package ru.fix.stdlib.concurrency.threads

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class ReschedulableSchedulerTest {

    @Test
    fun `fixed rate scheduling launches tasks`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())
        val latch = CountDownLatch(3)

        scheduler.schedule(
                DynamicProperty.of(Schedule.withRate(1)),
                0,
                Runnable {
                    latch.countDown()
                }
        )

        assertTrue(latch.await(10, SECONDS))

        scheduler.shutdown()
        assertTrue(scheduler.awaitTermination(10, SECONDS))
    }

    @Test
    fun `fixed delay scheduling launches tasks`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())
        val latch = CountDownLatch(3)

        scheduler.schedule(
                DynamicProperty.of(Schedule.withDelay(1)),
                0,
                Runnable {
                    latch.countDown()
                }
        )

        assertTrue(latch.await(10, SECONDS))

        scheduler.shutdown()
        assertTrue(scheduler.awaitTermination(10, SECONDS))
    }



    @Test
    fun `if task takes too long time to execute second task does not start in parallel`() {
        val scheduler = NamedExecutors.newScheduler("", DynamicProperty.of(5), NoopProfiler())

        val counter = AtomicInteger()

        val firstTaskIsRunning = AtomicBoolean(false)
        val firstTaskLatch = CountDownLatch(1)
        val secondTaskLatch = CountDownLatch(1)

        val secondTaskIsLaunchedInParallelWtihFirstLongTask = AtomicBoolean(false)

        scheduler.schedule(
                Schedule.withRate(DynamicProperty.of(1)),
                0,
                Runnable {
                    if (counter.getAndIncrement() == 0) {
                        //first task
                        firstTaskIsRunning.set(true)
                        Thread.sleep(500)
                        firstTaskIsRunning.set(false)
                        firstTaskLatch.countDown()
                    } else {
                        //other tasks that should not run
                        //while first one not finished
                        if (firstTaskIsRunning.get()) {
                            secondTaskIsLaunchedInParallelWtihFirstLongTask.set(true)
                        }
                        secondTaskLatch.countDown()
                    }
                }
        )

        assertTrue(firstTaskLatch.await(10, SECONDS))
        assertTrue(secondTaskLatch.await(10, SECONDS))

        scheduler.shutdown()
        assertTrue(scheduler.awaitTermination(10, SECONDS))

        assertFalse(secondTaskIsLaunchedInParallelWtihFirstLongTask.get())
    }

    @Test
    fun `job throws exception and scheduller continues to work`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())

        val latch = CountDownLatch(2)

        val schedule = scheduler.schedule(
                Schedule.withRate(DynamicProperty.of(1)),
                0,
                Runnable {
                    latch.countDown()
                    throw RuntimeException("unexpected exception")
                })

        assertTrue(latch.await(10, SECONDS))

        schedule.cancel(false)
        scheduler.shutdown()
        scheduler.awaitTermination(10, SECONDS)
    }

    @Test
    fun `delay dynamically changes when config changes`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("test-scheduler", NoopProfiler())
        val schedule = AtomicProperty(Schedule.withDelay(10))
        val invocations = LinkedBlockingDeque<Duration>()

        val prevRunTimestamp = AtomicReference(Instant.now())
        scheduler.schedule(schedule, 0) {

            val delay = Duration.between(prevRunTimestamp.get(), Instant.now())
            prevRunTimestamp.set(Instant.now())
            invocations.add(delay)
        }

        await().atMost(Duration.ofMinutes(10)).until {
            invocations.takeFirst() in (Duration.ofMillis(1)..Duration.ofMillis(1000))
        }
        schedule.set(Schedule.withDelay(1500))

        await().atMost(Duration.ofMinutes(10)).until {
            invocations.takeFirst() in (Duration.ofMillis(1000)..Duration.ofMillis(5000))
        }
    }

    @Test
    fun `reschedule must occur immediately if scheduled property changed`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())

        val taskExecutionCounter = AtomicInteger(0)

        val countDownLatchOfFirstTaskExecution = CountDownLatch(1)
        val countDownLatch = CountDownLatch(3)

        val property = AtomicProperty(TimeUnit.HOURS.toMillis(1))

        scheduler.schedule(Schedule.withDelay(property), Runnable {
            taskExecutionCounter.incrementAndGet()

            if (1 == taskExecutionCounter.get()) {
                countDownLatchOfFirstTaskExecution.countDown()
            } else {
                countDownLatch.countDown()
            }
        })

        assertTrue(countDownLatchOfFirstTaskExecution.await(20, SECONDS))
        assertEquals(1, taskExecutionCounter.get() )

        //Give scheduler time to schedule next task (task will be planned with hour dely)
        assertFalse(countDownLatch.await(1, SECONDS))


        //Change property, reschedule must occur
        property.set(100)

        assertTrue(countDownLatch.await(20, SECONDS))
        assertTrue(4 <= taskExecutionCounter.get())
    }
}