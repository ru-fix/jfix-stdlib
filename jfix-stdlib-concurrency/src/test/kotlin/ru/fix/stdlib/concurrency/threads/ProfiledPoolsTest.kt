package ru.fix.stdlib.concurrency.threads

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.greaterThanOrEqualTo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.commons.profiler.impl.SimpleProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

class ProfiledPoolsTest {

    @Test()
    fun `submit tasks, close pool and wait for termination`() {

        val profiler = SimpleProfiler()
        val reporter = profiler.createReporter()


        val pool = NamedExecutors.newDynamicPool("test", DynamicProperty.of(2), profiler)

        val taskCompleted = LongAdder()

        val taskStartedLatch = CountDownLatch(2)
        val unleashLatch = CountDownLatch(1)

        for (i in 1..100) {
            pool.submit {
                taskStartedLatch.countDown()
                unleashLatch.await()
                taskCompleted.increment()
            }
        }

        //wait for two task to start
        taskStartedLatch.await()


        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(98L, it.indicators["pool.test.queue.indicatorMax"])
            assertEquals(2L, it.indicators["pool.test.activeThreads.indicatorMax"])
        }

        //release tasks
        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        assert.that(100, equalTo(taskCompleted.sum()))

        reporter.buildReportAndReset().let {
            println(it)
            assertThat(100L, equalTo(it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCountSum))
            assertThat(0L, equalTo(it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCountMax))
            assertThat(98L, equalTo(it.profilerCallReports.find { it.name == "pool.test.await" }?.callsCountSum))
            assertThat(0L, equalTo(it.profilerCallReports.find { it.name == "pool.test.await" }?.activeCallsCountMax))
        }

    }


    @Test()
    fun `common pool indicators display common pool state`() {

        val profiler = SimpleProfiler()
        NamedExecutors.profileCommonPool(profiler)

        val reporter = profiler.createReporter()

        val taskStartedLatch = CountDownLatch(1)
        val unleashLatch = CountDownLatch(1)

        ForkJoinPool.commonPool().execute {

            Exception("I am running in the thread: ${Thread.currentThread().name} == ${Thread.currentThread().id}\n")
                    .printStackTrace()


            taskStartedLatch.countDown()
            unleashLatch.await()
        }

        taskStartedLatch.await()

        reporter.buildReportAndReset().let {
            println(it)
            assertThat(it.indicators["pool.commonPool.activeThread.indicatorMax"]!!, greaterThanOrEqualTo(1L))
        }

        unleashLatch.countDown()


        assertTrue { ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS) }

    }

    @Test()
    fun `single task submission`() {
        val profiler = SimpleProfiler()
        val reporter = profiler.createReporter()

        val pool = NamedExecutors.newDynamicPool("test", DynamicProperty.of(2), profiler)

        val taskCompleted = LongAdder()

        val taskStartedLatch = CountDownLatch(1)
        val unleashLatch = CountDownLatch(1)

        pool.submit {
            taskStartedLatch.countDown()
            unleashLatch.await()
            taskCompleted.increment()
        }

        taskStartedLatch.await()

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(0L, it.indicators["pool.test.queue.indicatorMax"])
            assertEquals(1L, it.indicators["pool.test.activeThreads.indicatorMax"])
        }

        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(1L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCountSum)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCountMax)
        }

        assertEquals(1, taskCompleted.sum())
    }

    @Test()
    fun `single task schedulling`() {
        val profiler = SimpleProfiler()
        val reporter = profiler.createReporter()

        val pool = NamedExecutors.newScheduledExecutor("test", DynamicProperty.of(2), profiler)

        val taskCompleted = LongAdder()

        val latch = CountDownLatch(1)

        pool.schedule(
                {
                    taskCompleted.increment()
                    latch.countDown()
                },
                1,
                TimeUnit.MILLISECONDS)

        latch.await()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(1L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCountSum)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCountMax)
        }

        assertEquals(1, taskCompleted.sum())
    }


    @Test()
    fun `schedule tasks, close pool and wait for termination`() {

        val profiler = SimpleProfiler()
        val reporter = profiler.createReporter()


        val pool = NamedExecutors.newScheduledExecutor("test", DynamicProperty.of(2), profiler)

        val taskCompleted = LongAdder()

        val latch = CountDownLatch(100)

        for (i in 1..100) {
            pool.schedule(
                    {
                        latch.countDown()
                        taskCompleted.increment()
                    },
                    1,
                    TimeUnit.MILLISECONDS)
        }

        latch.await()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(100L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCountSum)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCountMax)
        }

        assertEquals(100L, taskCompleted.sum())
    }
}

