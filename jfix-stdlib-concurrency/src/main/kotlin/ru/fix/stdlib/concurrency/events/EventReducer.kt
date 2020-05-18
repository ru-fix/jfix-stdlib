package ru.fix.stdlib.concurrency.events

import mu.KotlinLogging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

private const val DEFAULT_SHUTDOWN_CHECK_PERIOD_MS = 1_000L
private const val AWAIT_TERMINATION_PERIOD_MS = 60_000L

/**
 * EventReducer invokes given [handler] when [handleEvent] function was invoked with passing event through given [reduceFunction].
 * All events passed through [handleEvent] function before [handler] complete his invocation
 * will be accumulated in single butch of events by given [reduceFunction]. When [handler] completed his invocation,
 * it will immediately invoked again with that butch of events. And so on.
 *
 * Therefore if [handleEvent] function was invoked 100 times with breaks in 1 millisecond,
 * and [handler]'s work takes 70 milliseconds,
 * then [handler] wasn't invoked 100 times, but 3 times, consistently.
 *
 * Here is example:
 * ```
 *  val events = Array(500) { it } //numbers from 1 to 500
 *  EventReducer<Int, MutableList<Int>>(
 *      profiler = NoopProfiler(),
 *      reduceFunction = { accumulator, event ->
 *          accumulator?.apply { add(event!!) } ?: mutableListOf(event!!) //accumulate ints in list
 *      },
 *      handler = {
 *          println(it!!.size) //print received list size
 *      }
 *  ).use {
 *      it.start()
 *      for (event in events) {
 *          it.handleEvent(event) //send 500 events
 *      }
 *      Thread.sleep(3_000) //block thread to see what will be send to console
 *  }
 * ```
 * It will produce something like this: `1 382 13 15 17 27 45`.
 * Given [handler] was invoked about 7 times instead of 500, and all 500 numbers reached by [handler]
 * */
class EventReducer<ReceivingEventT, ReducedEventT>(
        profiler: Profiler,
        /**
         * rarely consistently invoked at the end of butch of events accumulated by [reduceFunction]
         * */
        private val handler: (ReducedEventT?) -> Unit,
        /**
         * frequently invoked for each new event to accumulate it in butch of events
         * */
        private val reduceFunction: (accumulatedEvent: ReducedEventT?, newEvent: ReceivingEventT?) -> ReducedEventT?,
        /**
         * how frequently thread checks if close method was invoked while waiting new event
         * */
        private val shutdownCheckPeriodMs: Long = DEFAULT_SHUTDOWN_CHECK_PERIOD_MS,
        /**
         * time to wait termination of [handler] invocation or checking if close method was invoked.
         * should be greater than [shutdownCheckPeriodMs]
         * */
        private val awaitTerminationPeriodMs: Long = AWAIT_TERMINATION_PERIOD_MS
) : AutoCloseable {
    private val eventReceivingExecutor = NamedExecutors.newSingleThreadPool(
            "event reducer", profiler
    )

    private val awaitingEventQueue = ArrayBlockingQueue<ReducedEventT?>(1)

    /**
     * Invoke this function for each new event. Thread-safe.
     * */
    fun handleEvent(event: ReceivingEventT? = null) = synchronized(awaitingEventQueue) {
        var accumulatedEvent: ReducedEventT? = awaitingEventQueue.poll()
        accumulatedEvent = reduceFunction.invoke(accumulatedEvent, event)
        awaitingEventQueue.put(accumulatedEvent)
    }

    /**
     * Launches sending accumulated events to [handler].
     * Until this function is called, all received events will be accumulated
     * */
    fun start() {
        eventReceivingExecutor.execute {
            while (true) {
                val event = awaitEventOrShutdown()
                if (event != null) {
                    handler.invoke(event)
                }
                if (eventReceivingExecutor.isShutdown) {
                    return@execute
                } //else unexpected exception, it's already logged
            }
        }
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
                handler: (EventT?) -> Unit,
                shutdownCheckPeriodMs: Long = DEFAULT_SHUTDOWN_CHECK_PERIOD_MS,
                awaitTerminationPeriodMs: Long = AWAIT_TERMINATION_PERIOD_MS
        ) = EventReducer(
                profiler = profiler,
                handler = handler,
                reduceFunction = { _: EventT?, event: EventT? -> event },
                shutdownCheckPeriodMs = shutdownCheckPeriodMs,
                awaitTerminationPeriodMs = awaitTerminationPeriodMs
        )
    }
}