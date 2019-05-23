package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class ReschedulableSchedulerTest {

    @Test
    fun `fixed rate scheduling`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())

        val latch = CountDownLatch(3)

        scheduler.schedule(
                DynamicProperty.of(Schedule.withRate(50)),
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
                        Thread.sleep(300)
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
    fun `job throws exception and scheduller continue to work`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())

        val latch = CountDownLatch(2)

        val schedule = scheduler.schedule(
                Schedule.withRate(DynamicProperty.of(50)),
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


    /**
     * Asserts dynamic delay change
     * Could be unstable.
     * Stable test for shceduller is hard to design
     * due to unpredictable nature of OS scheduler and current load of machine where test is executed.
     * Normal stable test is required.
     */
    @Test
    fun `dealy dynamically changes over time`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", AggregatingProfiler())

        val scheduleCounter = AtomicInteger()

        val scheduleSupplier = {
            when (scheduleCounter.getAndIncrement()) {
                0, 1 -> Schedule.withDelay(10)
                else -> Schedule.withDelay(1500)
            }
        }

        val prevRunTimestamp = AtomicReference(Instant.now())

        val countJobsLatch = CountDownLatch(2)
        val counter = AtomicInteger()

        val firstDelay = AtomicLong()
        val secondDelay = AtomicLong()


        scheduler.schedule(scheduleSupplier, 0) {
            val currentRunNum = counter.getAndIncrement()

            if (currentRunNum == 0) {
                //skip first run
                prevRunTimestamp.set(Instant.now())

            } else {
                val delay = Duration.between(prevRunTimestamp.get(), Instant.now()).toMillis()

                if (currentRunNum == 1) {
                    firstDelay.set(delay)

                } else if (currentRunNum == 2) {
                    secondDelay.set(delay)
                }
                println("run $currentRunNum delay: ${delay}ms")

                prevRunTimestamp.set(Instant.now())
                countJobsLatch.countDown()
            }
        }

        assertTrue(countJobsLatch.await(20, SECONDS))
        scheduler.shutdown()
        assertTrue(scheduler.awaitTermination(20, SECONDS))

        assertTrue(firstDelay.get() + 100 < secondDelay.get() - 100)
    }

    @Test
    fun `reschedule must occur immediately if scheduled property changed`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", AggregatingProfiler())

        val taskExecutionCounter = AtomicInteger(0)

        val countDownLatchOfFirstTaskExecution = CountDownLatch(1)
        val countDownLatch = CountDownLatch(3)

        val property = AtomicProperty(120000)

        scheduler.schedule(Schedule.withRate(property), 0, Runnable {
            taskExecutionCounter.incrementAndGet()

            if (1 == taskExecutionCounter.get()) {
                countDownLatchOfFirstTaskExecution.countDown()
            } else {
                countDownLatch.countDown()
            }
        })

        assertTrue(countDownLatchOfFirstTaskExecution.await(20, SECONDS))
        assertEquals(1, taskExecutionCounter.get() )

        //Change property, reschedule must occur
        property.set(500)

        assertTimeout(Duration.ofSeconds(20)) {
            assertTrue(countDownLatch.await(20, SECONDS))
            assertTrue(4 <= taskExecutionCounter.get())
        }
    }
}