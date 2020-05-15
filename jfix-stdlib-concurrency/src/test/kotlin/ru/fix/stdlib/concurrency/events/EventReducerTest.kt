package ru.fix.stdlib.concurrency.events

import org.junit.jupiter.api.Assertions.*
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
        EventReducer<Any>(profiler = NoopProfiler(), handler = {
            invokes.put(it)
        }).use {
            it.start()
            it.handleEvent(Any())
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            assertNull(invokes.poll(1, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `WHEN reducer triggered several times at once AND handler is slow THEN handler invoked one or two times`() {
        val eventsQuantity = 10
        val invokes = ArrayBlockingQueue<Any>(2)
        val awaitingEventsFromAllThreadsLatch = CountDownLatch(eventsQuantity)
        EventReducer<Any>(profiler = NoopProfiler(), handler = {
            invokes.put(it)
            awaitingEventsFromAllThreadsLatch.await() //simulate slow handler
        }).use { reducer ->
            reducer.start()

            val executor = Executors.newFixedThreadPool(eventsQuantity)
            repeat(eventsQuantity) {
                executor.execute {
                    reducer.handleEvent(Any())
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
        EventReducer<Any>(profiler = NoopProfiler(), handler = {
            invokes.put(it)
            holdHandlerInvocationLock.lock()
        }).use { reducer ->
            reducer.start()
            reducer.handleEvent(Any())
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            reducer.handleEvent(Any())
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
        val eventReducer = EventReducer<Any>(
                awaitTerminationPeriodMs = awaitTerminationPeriodMs,
                shutdownCheckPeriodMs = shutdownCheckPeriodMs,
                profiler = NoopProfiler(),
                handler = {
                    invokes.put(it)
                }
        )
        eventReducer.start()
        eventReducer.handleEvent(Any())
        assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
        eventReducer.close()
        assertNull(invokes.poll(1, TimeUnit.SECONDS))
    }

    @Test
    fun `WHEN custom reduce function provided THEN it used`() {
        val events = Array(50) { it }
        val invokes = ArrayBlockingQueue<Int>(events.size)
        EventReducer<Int>(
                profiler = NoopProfiler(),
                reduceFunction = { accumulator, event ->
                    event + (accumulator ?: 0)
                },
                handler = {
                    invokes.put(it)
                }
        ).use {
            it.start()

            for (event in events) {
                it.handleEvent(event)
            }

            val expectedEventSum = events.sum()
            var actualEventSum = 0
            while (actualEventSum < expectedEventSum) {
                actualEventSum += invokes.poll(1, TimeUnit.SECONDS)!!
            }
            assertNull(invokes.poll(1, TimeUnit.SECONDS))
            assertEquals(expectedEventSum, actualEventSum)
        }
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