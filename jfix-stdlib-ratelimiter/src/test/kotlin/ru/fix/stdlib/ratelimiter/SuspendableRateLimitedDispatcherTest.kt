package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import mu.KLogging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.aggregating.profiler.ProfilerReport
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//@Execution(ExecutionMode.CONCURRENT)
class SuspendableRateLimitedDispatcherTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val DISPATCHER_METRICS_PREFIX = "RateLimiterDispatcher.$DISPATCHER_NAME"
    }

    @Test
    fun `dispatch async operation with user defined async result type, operation invoked and it's result returned`() = runBlocking {
        class UserAsyncResult {
            fun whenComplete(callback: () -> Unit) {
                callback()
            }
        }

        val dispatcher = createDispatcher()

        val asyncResultInstance = UserAsyncResult()

        fun userAsyncOperation(): UserAsyncResult {
            return asyncResultInstance
        }

        val result = dispatcher.compose(
                { userAsyncOperation() },
                { asyncResult, callback -> asyncResult.whenComplete { callback.invoke() } }
        ).await()

        (result === asyncResultInstance).shouldBeTrue()

        dispatcher.close()
    }

    @Test
    fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`() = runBlocking {
        val dispatcher = createDispatcher()

        val operationResult = Object()
        fun userAsyncOperation(): CompletableFuture<Any> {
            return completedFuture(operationResult)
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        val future = delayedSubmissionFuture.await()
        future.isDone.shouldBeTrue()
        (future.get() === operationResult).shouldBeTrue()

        dispatcher.close()
    }


    @Test
    fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() = runBlocking {
        val dispatcher = createDispatcher()

        val asyncOperationException = Exception("some error")

        fun userAsyncOperation(): CompletableFuture<Any> {
            return CompletableFuture<Any>().apply {
                completeExceptionally(asyncOperationException)
            }
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }.await()

        await().atMost(Duration.ofSeconds(10)).until {
            delayedSubmissionFuture.isCompletedExceptionally
        }

        val actualException = shouldThrow<Exception> { delayedSubmissionFuture.get() }

        actualException.cause.shouldNotBeNull()
        actualException.cause.shouldBe(asyncOperationException)

        dispatcher.close()
    }


    @Test
    fun `if windows size is 0, then restricted only by limiter `() = runBlocking {
        `async operations are restricted by limiter limit `(0)
    }


    @Test
    fun `if window size is not empty and quite big, restricted by limiter`() = runBlocking {
        `async operations are restricted by limiter limit `(100_000)
    }

    private suspend fun `async operations are restricted by limiter limit `(windowSize: Int) {
        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                interations = ITERATIONS,
                windowSize = DynamicProperty.of(windowSize))

        val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

        logger.info("Throughput " + operationReport.stopThroughputAvg)
        operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
    }

    private suspend fun `submit series of operations`(
            ratePerSecond: Int,
            interations: Int,
            windowSize: DynamicProperty<Int>): ProfilerReport {


        val profiler = AggregatingProfiler()

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                window = windowSize,
                profiler = profiler
        )

        val counter = AtomicInteger(0)


        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val features = List(interations) {
            dispatcher.compose {
                profiledCall.profile<CompletableFuture<Int>> {
                    completedFuture(counter.incrementAndGet())
                }
            }.await()
        }

        logger.info("Submit $interations operations.")

        counter.get().shouldBe(interations)
        features.map { it.join() }.toSet().containsAll((1..interations).toList())

        val report = profilerReporter.buildReportAndReset()
        dispatcher.close()

        return report;
    }

    @Test
    fun `when window of uncompleted operations is full no new operation is dispatched`() = runBlocking {
        val dispatcher = TrackableDispatcher()
        dispatcher.windowProperty.set(10)

        dispatcher.submitTasks(1..10)
        delay(100)
        dispatcher.submitTasks(11..11)


        dispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        dispatcher.isSubmittedTaskInvoked(11).shouldBeFalse()

        dispatcher.completeTask(4)

        await().atMost(Duration.ofSeconds(10)).until {
            dispatcher.isSubmittedTaskInvoked(11)
        }

        dispatcher.completeAllAndClose()
    }

    @Test
    fun `'queue_wait', 'acquire_limit', 'acquire_window', 'supplied_operation', 'queue_size', 'active_async_operations' metrics gathered during execution`() = runBlocking {

        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                interations = ITERATIONS,
                windowSize = DynamicProperty.of(100))

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.queue_wait" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_window" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.queue_size")

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

        logger.info(report.toString())
    }

    @Test
    fun `indicators 'queue_size' and 'active_async_operations' adjusted according to number of queued and active operations`() = runBlocking {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.windowProperty.set(10)
        trackableDispatcher.submitTasks(1..12)
        await().atMost(2, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..10)
        }
        logger.info("CHECK 0")
        logger.info { "isSubmittedTaskInvoked: ${trackableDispatcher.isSubmittedTaskInvoked}" }
        reporter.buildReportAndReset().assertSoftly {
            indicators.mapKeys { it.key.name }.assertSoftly {
//                it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 1    //TODO to think after moving to kotlin semaphor
                it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 10
            }
            profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_window" }
//                    .activeCallsCountMax shouldBe 1
                    .activeCallsCountMax shouldBe 2
        }
        logger.info("CHECK 1")

        trackableDispatcher.completeTasks(1..10)
        await().atMost(3, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(11..12)
        }
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }
        logger.info("CHECK 2")

        delay(1000)
        trackableDispatcher.completeTasks(11..12)
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @Test
    fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() = runBlocking {
        val report = `submit series of operations`(500, 4000, DynamicProperty.of(1000))

        logger.info { report }
        report.indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }
        return@runBlocking
    }

    @Test
    fun `WHEN completed futures arrived THEN indicators are correctly adjusted`() = runBlocking {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.windowProperty.set(5)
        trackableDispatcher.submitCompletedTasks(1..4)
        trackableDispatcher.submitTasks(5..6)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..6)
        }
        logger.info("CHECK 0")
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }
        logger.info("CHECK 1")

        trackableDispatcher.submitCompletedTasks(7..10)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(7..10)
        }
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }
        logger.info("CHECK 2, ${reporter.buildReportAndReset().indicators}")

        delay(1000)
        trackableDispatcher.completeTasks(5..6)
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @Test
    fun `increasing window size allows to submit new operations up to the new limit`() = runBlocking {

        val trackableDispatcher = TrackableDispatcher()
        trackableDispatcher.windowProperty.set(10)

        trackableDispatcher.submitTasks(1..10)
        delay(200)
        trackableDispatcher.submitTasks(11..11)

        trackableDispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        trackableDispatcher.isSubmittedTaskInvoked(11).shouldBeFalse()

        trackableDispatcher.windowProperty.set(11)

        trackableDispatcher.submitTask(12, 1000)
        trackableDispatcher.completeTask(1)

        await().atMost(Duration.ofSeconds(10)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..12)
        }

        trackableDispatcher.completeAllAndClose();
    }


    @Test
    fun `decreasing window size reduces limit`() = runBlocking {
        val trackableDispatcher = TrackableDispatcher()
        trackableDispatcher.windowProperty.set(10)

        trackableDispatcher.submitTasks(1..10)

        delay(200)
        trackableDispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        trackableDispatcher.completeTasks(1..10)

        delay(1000)

        trackableDispatcher.windowProperty.set(4)
        trackableDispatcher.submitTasks(11..14)
        delay(200)
        trackableDispatcher.submitTasks(15..15)

        trackableDispatcher.isSubmittedTaskInvoked(11..14).shouldBeTrue()
        trackableDispatcher.isSubmittedTaskInvoked(15).shouldBeFalse()

        trackableDispatcher.completeTask(11)
        await().atMost(Duration.ofSeconds(10)).until {
            trackableDispatcher.isSubmittedTaskInvoked(15)
        }

        trackableDispatcher.completeAllAndClose();
    }


    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    fun `on shutdown fast tasks complete normally`() = runBlocking {

        val dispatch = createDispatcher(closingTimeout = 5_000)

//        assertTimeoutPreemptively(Duration.ofSeconds(10)) {

            val futures = List(3) {
                dispatch.compose {
                    completedFuture(true)
                }.await()
            }
            dispatch.close()

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeFalse()
            }
//        }
    }

    @Test
    fun `on shutdown slow tasks complete exceptionally`() = runBlocking {
        val dispatch = createDispatcher(closingTimeout = 0)

        val deferreds = ArrayList<Deferred<*>>()
            for (i in 1..3) {
                deferreds.add(dispatch.compose {
                    sleep(5000)
                    completedFuture(true)
                })
            }

            logger.info { "Closing dispatcher" }
            dispatch.close()

        deferreds.forEach { deferred ->
            shouldThrow<CancellationException> {
                deferred.await()
            }.message?.contains("Lol")
            deferred.isCancelled.shouldBeTrue()
        }
    }

    @Test
    fun `task, submitted in closed dispatcher, is rejected with exception`() = runBlocking {
        val dispatcher = createDispatcher()
        dispatcher.close()

        val t = shouldThrow<Exception> {
            dispatcher.compose {
                completedFuture(true)
            }.await()
        }
        t.shouldBeInstanceOf<RejectedExecutionException>()
        t.message!!.contains("TERMINATE")
        return@runBlocking
    }

    fun createDispatcher(
            rateLimitRequestPerSecond: Int = 500,
            window: DynamicProperty<Int> = DynamicProperty.of(0),
            closingTimeout: Int = 5000,
            profiler: Profiler = NoopProfiler()) =
            SuspendableRateLimitedDispatcher(
                    DISPATCHER_NAME,
                    ConfigurableRateLimiter("rate-limiter-name", rateLimitRequestPerSecond),
                    profiler,
                    window,
                    DynamicProperty.of(closingTimeout.toLong())
            )

    inner class TrackableDispatcher(
            profiler: Profiler = NoopProfiler()
    ) {

        val windowProperty = AtomicProperty(0)
        val dispatcher = createDispatcher(profiler = profiler, window = windowProperty)
        val submittedTasksResults = HashMap<Int, CompletableFuture<Any?>>() // TODO: to think about this map в конце
        val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()

        suspend fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        suspend fun submitTasks(tasks: IntRange, sleepTo: Long = 1000) {
            for (task in tasks) {
                submitTask(task, sleepTo)
            }
        }

        suspend fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, completedFuture(taskIndex), 0)

        suspend fun submitTask(taskIndex: Int, sleepTo: Long) = submitTask(taskIndex, CompletableFuture(), sleepTo)

        private suspend fun submitTask(taskIndex: Int, future: CompletableFuture<Any?>, sleepTo: Long) {
            submittedTasksResults[taskIndex] = future
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)

            dispatcher.compose {
                isSubmittedTaskInvoked[taskIndex]!!.set(true)
                logger.info("Task invoked $taskIndex")
                if (sleepTo > 0) {
                    sleep(Random.nextLong(500, sleepTo))
                }
                logger.info("Task done $taskIndex")
                future
            }
        }

        fun completeTask(taskIndex: Int) {
            submittedTasksResults[taskIndex]!!.complete(taskIndex)
        }

        fun completeTasks(range: IntRange) {
            for (task in range) {
                submittedTasksResults[task]!!.complete(task)
            }
        }

        fun isSubmittedTaskInvoked(index: Int) = isSubmittedTaskInvoked[index]!!.get()
        fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }

        fun completeAllAndClose() {
            submittedTasksResults.forEach { (_, future) -> future.complete(true) }

            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

    }

    @Test
    fun `invoke suspend function wrapped to completable future`() {
        val dispatcher = createDispatcher()

        val suspendFunctionInvoked = AtomicBoolean(false)

        suspend fun mySuspendFunction(): String{
            delay(500)
            suspendFunctionInvoked.set(true)
            return "suspend-function-result"
        }

        runBlocking {

            val result = dispatcher.compose { future { mySuspendFunction() } }.await().await()

            suspendFunctionInvoked.get().shouldBeTrue()
            result.shouldBe("suspend-function-result")

        }
    }
}
