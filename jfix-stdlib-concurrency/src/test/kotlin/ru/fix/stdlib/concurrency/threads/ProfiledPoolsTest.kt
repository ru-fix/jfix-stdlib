package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.commons.profiler.impl.SimpleProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture
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
            assertEquals(98L, it.indicators["pool.test.queue"])
            assertEquals(2L, it.indicators["pool.test.activeThreads"])
        }

        //release tasks
        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        assertEquals(100, taskCompleted.sum())

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(100L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCount)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCount)
            assertEquals(98L, it.profilerCallReports.find { it.name == "pool.test.await" }?.callsCount)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.await" }?.activeCallsCount)
        }

    }


    @Test()
    fun `common pool indicators display common pool state`() {

        val profiler = SimpleProfiler()
        NamedExecutors.profileCommonPool(profiler)

        val reporter = profiler.createReporter()

        val taskCompleted = LongAdder()

        val taskStartedLatch = CountDownLatch(1)
        val unleashLatch = CountDownLatch(1)

        CompletableFuture.runAsync {
            taskStartedLatch.countDown()
            unleashLatch.await()
            taskCompleted.increment()
        }

        taskStartedLatch.await()

        reporter.buildReportAndReset().let {
            println(it)
            assertTrue { it.indicators["pool.commonPool.activeThread"]!! >= 1 }
        }

        unleashLatch.countDown()


        assertTrue { ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS) }

        assertEquals(1, taskCompleted.sum())
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
            assertEquals(0L, it.indicators["pool.test.queue"])
            assertEquals(1L, it.indicators["pool.test.activeThreads"])
        }

        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(1L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCount)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCount)
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
            assertEquals(1L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCount)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCount)
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
            assertEquals(100L, it.profilerCallReports.find { it.name == "pool.test.run" }?.callsCount)
            assertEquals(0L, it.profilerCallReports.find { it.name == "pool.test.run" }?.activeCallsCount)
        }

        assertEquals(100L, taskCompleted.sum())
    }
}

