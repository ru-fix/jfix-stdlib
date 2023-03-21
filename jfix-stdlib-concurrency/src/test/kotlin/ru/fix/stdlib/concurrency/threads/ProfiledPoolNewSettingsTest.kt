package ru.fix.stdlib.concurrency.threads

import io.kotest.assertions.assertSoftly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.Identity
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

internal class ProfiledPoolNewSettingsTest {

    @Test
    fun `submit tasks, close pool and wait for termination`() {

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()

        val settings = ProfiledThreadPoolSettings(
            corePoolSize = 2,
            maxPoolSize = 2
        )
        val pool = NamedExecutors.newDynamicPool(
            DynamicProperty.of(settings),
            "test",
            profiler
        )

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


        reporter.buildReportAndReset().assertSoftly {
            println(it)
            it.indicators[Identity("pool.test.queue")] shouldBe 98L
            it.indicators[Identity("pool.test.activeThreads")] shouldBe 2L
            it.indicators[Identity("pool.test.poolSize")] shouldBe 2
            it.indicators[Identity("pool.test.maxPoolSize")] shouldBe 2
        }

        //release tasks
        unleashLatch.countDown()

        pool.shutdown()

        pool.awaitTermination(20, TimeUnit.SECONDS) shouldBe true

        taskCompleted.sum() shouldBe 100L

        reporter.buildReportAndReset().let { report ->
            println(report)
            report.profilerCallReports.find {
                it.identity.name == "pool.test.run"
            }?.assertSoftly {
                it.stopSum shouldBe 100L
                it.activeCallsCountMax shouldBe 0L
            }

            report.profilerCallReports.find {
                it.identity.name == "pool.test.await"
            }?.assertSoftly {
                it.stopSum shouldBe 98L
                it.activeCallsCountMax shouldBe 0L
            }
        }
    }

    @Test
    fun `single task submission`() {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()

        val settings = ProfiledThreadPoolSettings(
            corePoolSize = 1,
            maxPoolSize = 2
        )
        val pool = NamedExecutors.newDynamicPool(
            DynamicProperty.of(settings),
            "test",
            profiler
        )

        val taskCompleted = LongAdder()

        val taskStartedLatch = CountDownLatch(1)
        val unleashLatch = CountDownLatch(1)

        pool.submit {
            taskStartedLatch.countDown()
            unleashLatch.await()
            taskCompleted.increment()
        }

        taskStartedLatch.await()

        reporter.buildReportAndReset().assertSoftly {
            println(it)
            it.indicators[Identity("pool.test.queue")] shouldBe 0L
            it.indicators[Identity("pool.test.activeThreads")] shouldBe 1L
        }

        unleashLatch.countDown()

        pool.shutdown()

        pool.awaitTermination(10, TimeUnit.SECONDS) shouldBe true

        reporter.buildReportAndReset().assertSoftly {
            println(it)
            it.profilerCallReports.find { report ->
                report.identity.name == "pool.test.run"
            }?.stopSum shouldBe 1L

            it.profilerCallReports.find { report ->
                report.identity.name == "pool.test.run"
            }?.activeCallsCountMax shouldBe 0L
        }

        taskCompleted.sum() shouldBe 1
    }

    @Test
    fun `dynamic pool inherits pool's classloader, instead of caller thread`() {
        val settings = ProfiledThreadPoolSettings(
            corePoolSize = 1,
            maxPoolSize = 2
        )
        val pool = NamedExecutors.newDynamicPool(
            DynamicProperty.of(settings),
            "my-pool",
            NoopProfiler()
        )

        fun newProfilerInstance(): Any {
            // loading via thread context class loader (TCCL)
            return Thread.currentThread().contextClassLoader
                .loadClass(NoopProfiler::class.qualifiedName)
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
}
