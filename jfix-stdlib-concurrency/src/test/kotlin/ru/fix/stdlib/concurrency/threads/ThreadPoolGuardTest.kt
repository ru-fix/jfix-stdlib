package ru.fix.stdlib.concurrency.threads

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.containsSubstring
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class ThreadPoolGuardTest {

    @Test
    fun `common pool not blocked and  queue size is within boundaries`() {

        val semaphore = Semaphore(0)

        val alarmFlag = AtomicBoolean()

        val pool = ForkJoinPool(4)

        val guard = ForkJoinThreadPoolGuard(
                NoopProfiler(),
                Schedule.withRate(DynamicProperty.of(100L)),
                pool,
                DynamicProperty.of(400)) { queueSize, dump ->
            alarmFlag.set(true)
        }

        val futures = mutableListOf<CompletableFuture<*>>()

        for (i in 1..300) {
            futures.add(
                    CompletableFuture.runAsync(Runnable {
                        semaphore.acquire()
                    }, pool)
            )
        }

        //Wait for 1 second and assure that guard did not raised alarm
        Thread.sleep(1000)
        semaphore.release(300)

        futures.forEach { it.join() }

        Assertions.assertFalse(alarmFlag.get())
    }


    @Test
    fun `common pool is blocked and queue size out of boundaries`() {

        val semaphore = Semaphore(0)

        val alarmFlag = AtomicBoolean()
        val dumpReceiver = AtomicReference<String>()

        val pool = ForkJoinPool(4)

        val guard = ForkJoinThreadPoolGuard(
                NoopProfiler(),
                Schedule.withRate(DynamicProperty.of(100L)),
                pool,
                DynamicProperty.of(400)) { queueSize, dump ->

            if (alarmFlag.compareAndSet(false, true)) {
                dumpReceiver.set(dump)
            }
        }

        val futures = mutableListOf<CompletableFuture<*>>()

        for (i in 1..500) {
            futures.add(
                    CompletableFuture.runAsync(Runnable {
                        semaphore.acquire()
                    }, pool)
            )
        }

        await().atMost(Duration.ofMinutes(1)).until(alarmFlag::get)

        semaphore.release(500)
        futures.forEach { it.join() }

        assertThat(dumpReceiver.get(), containsSubstring(this::class.java.simpleName))
        assertThat(dumpReceiver.get(), containsSubstring("acquire"))
    }
}