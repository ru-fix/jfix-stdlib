package ru.fix.stdlib.concurrency.threads

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.greaterThanOrEqualTo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.Identity
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

class ProfiledPoolsTest {

    @Test
    fun `submit tasks, close pool and wait for termination`() {

        val profiler = AggregatingProfiler()
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
            assertEquals(98L, it.indicators[Identity("pool.test.queue")])
            assertEquals(2L, it.indicators[Identity("pool.test.activeThreads")])
            assertEquals(2, it.indicators[Identity("pool.test.poolSize")])
            assertEquals(2, it.indicators[Identity("pool.test.maxPoolSize")])
        }

        //release tasks
        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(20, TimeUnit.SECONDS) }

        assert.that(100L, equalTo(taskCompleted.sum()))

        reporter.buildReportAndReset().let { report ->
            println(report)
            val runReport = report.profilerCallReports.find { it.identity.name == "pool.test.run" }
            assertThat(100L, equalTo(runReport?.stopSum))
            assertThat(0L, equalTo(runReport?.activeCallsCountMax))
            val awaitReport = report.profilerCallReports.find { it.identity.name == "pool.test.await" }
            assertThat(98L, equalTo(awaitReport?.stopSum))
            assertThat(0L, equalTo(awaitReport?.activeCallsCountMax))
        }

    }

    @Test
    fun `common pool indicators display common pool state`() {

        val profiler = AggregatingProfiler()
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
            assertThat(it.indicators[Identity("pool.commonPool.activeThread")]!!, greaterThanOrEqualTo(1L))
        }

        unleashLatch.countDown()


        assertTrue { ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS) }

    }

    @Test
    fun `single task submission`() {
        val profiler = AggregatingProfiler()
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
            assertEquals(0L, it.indicators[Identity("pool.test.queue")])
            assertEquals(1L, it.indicators[Identity("pool.test.activeThreads")])
        }

        unleashLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let {
            println(it)
            assertEquals(1L, it.profilerCallReports.find { it.identity.name == "pool.test.run" }?.stopSum)
            assertEquals(0L, it.profilerCallReports.find { it.identity.name == "pool.test.run" }?.activeCallsCountMax)
        }

        assertEquals(1, taskCompleted.sum())
    }

    @Test
    fun `single task schedulling`() {
        val profiler = AggregatingProfiler()
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
            assertEquals(1L, it.profilerCallReports.find { it.identity.name == "pool.test.run" }?.stopSum)
            assertEquals(0L, it.profilerCallReports.find { it.identity.name == "pool.test.run" }?.activeCallsCountMax)
        }

        assertEquals(1, taskCompleted.sum())
    }


    @Test
    fun `schedule tasks, close pool and wait for termination`() {

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()


        val pool = NamedExecutors.newScheduledExecutor("test", DynamicProperty.of(2), profiler)

        val taskCompleted = LongAdder()

        val latch = CountDownLatch(100)
        val releasePoolLatch = CountDownLatch(1)

        for (i in 1..100) {
            pool.schedule(
                    {
                        latch.countDown()
                        taskCompleted.increment()
                        if (i >= 99) {
                            releasePoolLatch.await()
                        }
                    },
                    1,
                    TimeUnit.MILLISECONDS)
        }

        latch.await()

        reporter.buildReportAndReset { metric, _ ->
            metric.name in setOf(
                    "pool.test.queue",
                    "pool.test.activeThreads",
                    "pool.test.poolSize",
                    "pool.test.maxPoolSize"
            )
        }.let {
            println(it)
            assertEquals(0L, it.indicators[Identity("pool.test.queue")])
            assertEquals(2L, it.indicators[Identity("pool.test.activeThreads")])
            assertEquals(2L, it.indicators[Identity("pool.test.poolSize")])
            assertEquals(2L, it.indicators[Identity("pool.test.maxPoolSize")])
        }

        releasePoolLatch.countDown()

        pool.shutdown()

        assertTrue { pool.awaitTermination(10, TimeUnit.SECONDS) }

        reporter.buildReportAndReset().let { report ->
            println(report)
            val runReport = report.profilerCallReports.find { it.identity.name == "pool.test.run" }
            assertEquals(100L, runReport?.stopSum)
            assertEquals(0L, runReport?.activeCallsCountMax)
        }

        assertEquals(100L, taskCompleted.sum())
    }

    @Test
    fun `dynamic pool inherits pool's classloader, instead of caller thread`() {
        val profiler = NoopProfiler()
        val pool = NamedExecutors.newDynamicPool("my-pool", DynamicProperty.of(2), profiler)

        fun newProfilerInstance(): Any {
            // loading via thread context class loader (TCCL)
            return Thread.currentThread().contextClassLoader.loadClass(NoopProfiler::class.qualifiedName)
                    .getDeclaredConstructor().newInstance()
        }

        pool.submit {
            newProfilerInstance()
        }.get(1, TimeUnit.SECONDS)

        val tccl = Thread.currentThread().contextClassLoader
        try {
            // reset current tccl (or set any classloader, which can't load required class)
            Thread.currentThread().contextClassLoader = null

            // submitting new task, pool hasn't yet inflated, so new thread will be started
            pool.submit {
                newProfilerInstance()
                // we still can instantiate class, because contextClassLoader was inherited
                // from executorService's class loader, not caller thread's context class loader
            }.get(1, TimeUnit.SECONDS)
        } finally {
            // restore tccl
            Thread.currentThread().contextClassLoader = tccl
        }
    }

    @Test
    fun `scheduled pool inherits pool's classloader, instead of caller thread`() {
        val profiler = NoopProfiler()
        val pool = NamedExecutors.newSingleThreadScheduledExecutor("my-pool", profiler)

        fun newProfilerInstance(): Any {
            // loading via thread context class loader (TCCL)
            return Thread.currentThread().contextClassLoader.loadClass(NoopProfiler::class.qualifiedName)
                    .getDeclaredConstructor().newInstance()
        }

        val tccl = Thread.currentThread().contextClassLoader
        try {
            // reset current tccl (or set any classloader, which can't load required class)
            Thread.currentThread().contextClassLoader = null

            // submitting new task, pool hasn't yet inflated, so new thread will be started
            pool.submit {
                newProfilerInstance()
                // we still can instantiate class, because contextClassLoader was inherited
                // from executorService's class loader, not caller thread's context class loader
            }.get(1, TimeUnit.SECONDS)
        } finally {
            // restore tccl
            Thread.currentThread().contextClassLoader = tccl
        }
    }
}

