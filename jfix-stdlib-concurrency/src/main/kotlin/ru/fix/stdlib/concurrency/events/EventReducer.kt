package ru.fix.stdlib.concurrency.events

import mu.KotlinLogging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

private const val DEFAULT_SHUTDOWN_CHECK_PERIOD_MS = 1_000L
private const val AWAIT_TERMINATION_PERIOD_MS = 60_000L

/**
 * EventReducer invokes given [handler] when [handleEvent] function was invoked.
 * If [handleEvent] function was invoked (one or many times) during [handler] working,
 * [handler] will be invoked after completing and so on.
 *
 * Therefore if [handleEvent] function was invoked 100 times with breaks in 1 millisecond,
 * and [handler]'s work takes 70 milliseconds,
 * then [handler] wasn't invoked 100 times, but 3 times, consistently.
 * */
class EventReducer<ReceivingEventT, ReducedEventT>(
        profiler: Profiler,
        private val handler: (ReducedEventT) -> Unit,
        private val reduceFunction: (accumulatedEvent: ReducedEventT?, newEvent: ReceivingEventT) -> ReducedEventT,
        private val shutdownCheckPeriodMs: Long = DEFAULT_SHUTDOWN_CHECK_PERIOD_MS,
        private val awaitTerminationPeriodMs: Long = AWAIT_TERMINATION_PERIOD_MS
) : AutoCloseable {
    private val eventReceivingExecutor = NamedExecutors.newSingleThreadPool(
            "event reducer", profiler
    )

    private val awaitingEventQueue = ArrayBlockingQueue<ReducedEventT>(1)

    fun handleEvent(event: ReceivingEventT) = synchronized(awaitingEventQueue) {
        var accumulatedEvent: ReducedEventT? = awaitingEventQueue.poll()
        accumulatedEvent = reduceFunction.invoke(accumulatedEvent, event)
        awaitingEventQueue.put(accumulatedEvent!!)
    }

    fun start() {
        eventReceivingExecutor.submit(Runnable {
            while (true) {
                val event = awaitEventOrShutdown()
                if (event != null) {
                    handler.invoke(event)
                }
                if (eventReceivingExecutor.isShutdown) {
                    return@Runnable
                } //else unexpected exception, it's already logged
            }
        })
    }

    private fun awaitEventOrShutdown(): ReducedEventT? {
        try {
            var receivedEvent: ReducedEventT? = null
            while (receivedEvent == null) {
                receivedEvent = awaitingEventQueue.poll(shutdownCheckPeriodMs, TimeUnit.MILLISECONDS)
                if (eventReceivingExecutor.isShutdown) {
                    return null
                }
            }
            return receivedEvent
        } catch (e: Exception) {
            log.error("waiting event was interrupted", e)
        }
        return null
    }

    override fun close() {
        eventReceivingExecutor.shutdown()
        if (!eventReceivingExecutor.awaitTermination(awaitTerminationPeriodMs, TimeUnit.MILLISECONDS)) {
            log.warn("Failed to wait eventReceivingExecutor termination")
            eventReceivingExecutor.shutdownNow()
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}

        @JvmStatic
        fun <EventT> lastEventWinReducer(
                profiler: Profiler,
                handler: (EventT) -> Unit,
                shutdownCheckPeriodMs: Long = DEFAULT_SHUTDOWN_CHECK_PERIOD_MS,
                awaitTerminationPeriodMs: Long = AWAIT_TERMINATION_PERIOD_MS
        ) = EventReducer(
                profiler = profiler,
                handler = handler,
                reduceFunction = { _: EventT?, event: EventT -> event },
                shutdownCheckPeriodMs = shutdownCheckPeriodMs,
                awaitTerminationPeriodMs = awaitTerminationPeriodMs
        )
    }
}