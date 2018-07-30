package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import ru.fix.commons.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

class ThreadPoolGuardTest {

    @Test
    fun `common pool not blocked and  queue size is within boundaries`() {

        val semaphore = Semaphore(0)

        val alarmFlag = AtomicBoolean()

        val guard = CommonThreadPoolGuard(
                NoopProfiler(),
                Schedule.withRate(DynamicProperty.of(100L)),
                DynamicProperty.of(400)) { queueSize, dump ->
            alarmFlag.set(true)
        }

        val futures = mutableListOf<CompletableFuture<*>>()

        for (i in 1..300) {
            futures.add(CompletableFuture.runAsync {
                semaphore.acquire()
            })
        }

        Thread.sleep(300)
        semaphore.release(300)

        futures.forEach { it.join() }

        Assertions.assertEquals(false, alarmFlag.get())
    }


    @Test
    fun `common pool is blocked and queue size out of boundaries`() {

        val semaphore = Semaphore(0)

        val alarmFlag = AtomicBoolean()

        val guard = CommonThreadPoolGuard(
                NoopProfiler(),
                Schedule.withRate(DynamicProperty.of(100L)),
                DynamicProperty.of(400)) { queueSize, dump ->

            println(dump)
            alarmFlag.set(true)
        }

        val futures = mutableListOf<CompletableFuture<*>>()

        for (i in 1..500) {
            futures.add(CompletableFuture.runAsync {
                semaphore.acquire()
            })
        }

        Thread.sleep(300)
        semaphore.release(500)

        futures.forEach { it.join() }

        Assertions.assertEquals(true, alarmFlag.get())
    }
}