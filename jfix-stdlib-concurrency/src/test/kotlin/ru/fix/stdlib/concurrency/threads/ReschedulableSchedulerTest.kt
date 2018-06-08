package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.commons.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ReschedulableSchedulerTest {
    @Test
    fun `job throws exception and scheduller continue to work`() {
        val scheduler = NamedExecutors.newSingleThreadScheduler("", NoopProfiler())

        val latch = CountDownLatch(3)

        val schedule = scheduler.schedule(
                Schedule.withRate(DynamicProperty.of(100)),
                0,
                Runnable {
                    latch.countDown()
                    throw RuntimeException("unexpected exception")
                })

        assertTrue(latch.await(10, TimeUnit.SECONDS))

        schedule.cancel(false)
        scheduler.shutdownNow()
    }
}