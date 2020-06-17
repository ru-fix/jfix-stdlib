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

private const val INVOCATION_TIMEOUT_MS = 500L

internal class ReducingEventAccumulatorTest {

    @Test
    fun `WHEN handle single time THEN handler invoked one time`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        ReducingEventAccumulator.lastEventWinAccumulator<Any>(profiler = NoopProfiler(), handler = {
            invokes.put(it)
        }).use {
            it.start()
            it.handleEvent(Any())
            assertNotNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            assertNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test
    fun `WHEN reducer triggered several times at once AND handler is slow THEN handler invoked one or two times`() {
        val eventsQuantity = 10
        val invokes = ArrayBlockingQueue<Any>(2)
        val awaitingEventsFromAllThreadsLatch = CountDownLatch(eventsQuantity)
        ReducingEventAccumulator.lastEventWinAccumulator<Any>(profiler = NoopProfiler(), handler = {
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

            assertNotNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            assertNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))

            executor.shutdown()
        }
    }

    @Test
    fun `WHEN handle event after invoking handler THEN handler will be invoked after completing`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        val holdHandlerInvocationLock = ReentrantLock().apply { lock() }
        ReducingEventAccumulator.lastEventWinAccumulator<Any>(profiler = NoopProfiler(), handler = {
            invokes.put(it)
            holdHandlerInvocationLock.lock()
        }).use { reducer ->
            reducer.start()
            reducer.handleEvent(Any())
            assertNotNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            reducer.handleEvent(Any())
            holdHandlerInvocationLock.unlock()
            assertNotNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @ParameterizedTest
    @MethodSource("awaitTerminationPeriodMs And ShutdownCheckPeriodMs")
    fun `WHEN reducer was shutdown THEN handler not invoked`(
            awaitTerminationPeriodMs: Long, shutdownCheckPeriodMs: Long
    ) {
        val invokes = ArrayBlockingQueue<Any>(1)
        val eventReducer = ReducingEventAccumulator.lastEventWinAccumulator<Any>(
                awaitTerminationPeriodMs = awaitTerminationPeriodMs,
                shutdownCheckPeriodMs = shutdownCheckPeriodMs,
                profiler = NoopProfiler(),
                handler = { invokes.put(it) }
        )
        eventReducer.start()
        eventReducer.handleEvent(Any())
        assertNotNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
        eventReducer.close()
        assertNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
    }

    @Test
    fun `WHEN receiving and reduced event types are equal AND custom reduce function provided THEN it used`() {
        val events = Array(50) { it }
        val invokes = ArrayBlockingQueue<Int>(events.size)
        ReducingEventAccumulator<Int, Int>(
                profiler = NoopProfiler(),
                reduceFunction = { accumulator, event -> event + (accumulator ?: 0) },
                handler = { invokes.put(it) }
        ).use {
            it.start()

            for (event in events) {
                it.handleEvent(event)
            }

            val expectedEventSum = events.sum()
            var actualEventSum = 0
            while (actualEventSum < expectedEventSum) {
                actualEventSum += invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)!!
            }
            assertNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            assertEquals(expectedEventSum, actualEventSum)
        }
    }

    @Test
    fun `WHEN receiving and reduced event types are different AND custom reduce function provided THEN it used`() {
        val events = Array(50) { it }
        val invokes = ArrayBlockingQueue<List<Int>>(events.size)
        ReducingEventAccumulator<Int, MutableList<Int>>(
                profiler = NoopProfiler(),
                reduceFunction = { accumulator, event ->
                    accumulator?.apply { add(event) } ?: mutableListOf(event)
                },
                handler = { invokes.put(it) }
        ).use {
            it.start()

            for (event in events) {
                it.handleEvent(event)
            }

            val receivedEvents = mutableListOf<Int>()
            while (receivedEvents.size < events.size) {
                receivedEvents.addAll(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)!!)
            }
            assertNull(invokes.poll(INVOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            assertEquals(events.toMutableList(), receivedEvents)
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