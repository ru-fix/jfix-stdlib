package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.AtomicProperty
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

class ChangePoolSizeTest {

    @Test
    fun `increase dynamic pool size`() {
        val size = AtomicProperty(5)
        val pool = NamedExecutors.newDynamicPool("test", size, NoopProfiler())
        size.set(10)
        checkPossibilityToLaunchNConcurrentThreads(10, pool)
    }

    private fun checkPossibilityToLaunchNConcurrentThreads(numberOfThreads: Int, pool: ExecutorService){
        val latch = CountDownLatch(numberOfThreads)
        (1..numberOfThreads).map {
            pool.submit {
                latch.countDown()
                latch.await()
            }
        }.forEach { it.get(1, TimeUnit.MINUTES) }
    }

    @Test
    fun `decrease dynamic pool size`() {
        val size = AtomicProperty(5)
        val pool = NamedExecutors.newDynamicPool("test", size, NoopProfiler())
        size.set(3)

        checkPossibilityToLaunchNConcurrentThreads(3, pool)
    }

    @Test
    fun `increase scheduled pool size`() {
        val size = AtomicProperty(5)
        val pool = NamedExecutors.newScheduledExecutor("test", size, NoopProfiler())
        size.set(10)

        checkPossibilityToLaunchNConcurrentThreads(10, pool)
    }

    @Test
    fun `decrease scheduled pool size`() {
        val size = AtomicProperty(5)
        val pool = NamedExecutors.newScheduledExecutor("test", size, NoopProfiler())
        size.set(3)

        checkPossibilityToLaunchNConcurrentThreads(3, pool)
    }
}