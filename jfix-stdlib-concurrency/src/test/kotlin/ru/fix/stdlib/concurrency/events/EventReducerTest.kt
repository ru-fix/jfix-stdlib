package ru.fix.stdlib.concurrency.events

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.fix.aggregating.profiler.NoopProfiler
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

internal class EventReducerTest {

    @Test
    fun `WHEN handle single time THEN handler invoked one time`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        EventReducer(profiler = NoopProfiler(), handler = {
            invokes.put(Any())
        }).use {
            it.start()
            it.handle()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            assertNull(invokes.poll(1, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `WHEN reducer triggered several times at once AND handler is slow THEN handler invoked one or two times`() {
        val eventsQuantity = 10
        val invokes = ArrayBlockingQueue<Any>(2)
        val awaitingEventsFromAllThreadsLatch = CountDownLatch(eventsQuantity)
        EventReducer(profiler = NoopProfiler(), handler = {
            invokes.put(Any())
            awaitingEventsFromAllThreadsLatch.await() //simulate slow handler
        }).use { reducer ->
            reducer.start()

            val executor = Executors.newFixedThreadPool(eventsQuantity)
            repeat(eventsQuantity) {
                executor.execute {
                    reducer.handle()
                    awaitingEventsFromAllThreadsLatch.countDown()
                }
            }

            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            invokes.poll(1, TimeUnit.SECONDS)
            assertNull(invokes.poll(1, TimeUnit.SECONDS))

            executor.shutdown()
        }
    }

    @Test
    fun `WHEN handle event after invoking handler THEN handler will be invoked after completing`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        val holdHandlerInvocationLock = ReentrantLock().apply { lock() }
        EventReducer(profiler = NoopProfiler(), handler = {
            invokes.put(Any())
            holdHandlerInvocationLock.lock()
        }).use { reducer ->
            reducer.start()
            reducer.handle()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            reducer.handle()
            holdHandlerInvocationLock.unlock()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
        }
    }

    @ParameterizedTest
    @MethodSource("awaitTerminationPeriodMs And ShutdownCheckPeriodMs")
    fun `WHEN reducer was shutdown THEN handler not invoked`(
            awaitTerminationPeriodMs: Long, shutdownCheckPeriodMs: Long
    ) {
        val invokes = ArrayBlockingQueue<Any>(1)
        val eventReducer = EventReducer(
                awaitTerminationPeriodMs = awaitTerminationPeriodMs,
                shutdownCheckPeriodMs = shutdownCheckPeriodMs,
                profiler = NoopProfiler(),
                handler = {
                    invokes.put(Any())
                }
        )
        eventReducer.start()
        eventReducer.handle()
        assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
        eventReducer.close()
        assertNull(invokes.poll(1, TimeUnit.SECONDS))
    }

    @Suppress("unused")
    companion object {
        @JvmStatic
        fun `awaitTerminationPeriodMs And ShutdownCheckPeriodMs`() = listOf(
                Arguments.of(60_000L, 1_000L),
                Arguments.of(1L, 1_000L)
        )
    }
}