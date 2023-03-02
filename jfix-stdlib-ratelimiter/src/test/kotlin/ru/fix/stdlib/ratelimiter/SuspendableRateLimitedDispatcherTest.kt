package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.longs.shouldBeInRange
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import mu.KLogging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class SuspendableRateLimitedDispatcherTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val DISPATCHER_METRICS_PREFIX = "RateLimiterDispatcher.$DISPATCHER_NAME"
    }

    val scope: CoroutineScope = DispatcherCommonPoolScope

    @Test
    fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`() = runBlocking {
        val dispatcher = createDispatcher()

        val operationResult = Object()
        fun userAsyncOperation(): CompletableFuture<Any> {
            return completedFuture(operationResult)
        }

        val future = dispatcher.compose { userAsyncOperation() }

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

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        await().atMost(Duration.ofSeconds(10)).until {
            delayedSubmissionFuture.isCompletedExceptionally
        }

        val actualException = shouldThrow<Exception> { delayedSubmissionFuture.get() }

        actualException.cause.shouldNotBeNull()
        actualException.cause.shouldBe(asyncOperationException)

        dispatcher.close()
    }

    @Test
    fun `series of operations test`() = runBlocking {
        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                interations = ITERATIONS
        )

        val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

        logger.info("Throughput " + operationReport.stopThroughputAvg)
        operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
    }

    private suspend fun `submit series of operations`(
            ratePerSecond: Int,
            interations: Int
    ): ProfilerReport {

        val profiler = AggregatingProfiler()

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
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
            }
        }

        counter.get().shouldBe(interations)
        features.map { it.join() }.toSet().containsAll((1..interations).toList())

        val report = profilerReporter.buildReportAndReset()
        dispatcher.close()

        return report;
    }

    @Test
    fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution`() = runBlocking {

        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                interations = ITERATIONS
        )

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

        logger.info(report.toString())
    }

    @Test
    fun `indicators 'active_async_operations' adjusted according to number of queued and active operations`() = runBlocking {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.submitTasks(1..10, 11..12)
        await().atMost(Duration.ofSeconds(2)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..10)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 12//10
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..10)
        }
        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcher.isSubmittedTaskInvoked(11..12)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBeInRange LongRange(0, 2)//2
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..12)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @Test
    fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() = runBlocking {
        val report = `submit series of operations`(500, 4000)

        logger.info { report }
        report.assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }
        return@runBlocking
    }

    @Test
    fun `WHEN completed futures arrived THEN indicators are correctly adjusted`() = runBlocking {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.submitCompletedTasks(1..4)
        trackableDispatcher.submitTasks(5..6, 3000) // long executed tasks
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..6)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        trackableDispatcher.submitCompletedTasks(7..10)
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskInvoked(7..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `on shutdown fast tasks complete normally`() = runBlocking {

        val dispatch = createDispatcher(closingTimeout = 5_000)

        val futures = List(3) {
            dispatch.compose {
                completedFuture(true)
            }
        }
        dispatch.close()

        futures.forEach { future: CompletableFuture<*> ->
            future.isDone.shouldBeTrue()
            future.isCompletedExceptionally.shouldBeFalse()
        }
    }

    @Test
    @Timeout(5, unit = TimeUnit.SECONDS)
    fun `on shutdown slow tasks complete exceptionally`() = runBlocking {
        val dispatch = createDispatcher(closingTimeout = 0)

        val deferreds = ArrayList<Deferred<*>>()
        for (i in 1..3) {
            deferreds.add(
                    scope.async {
                        dispatch.compose {
                            sleep(3000)
                            completedFuture(true)
                        }
                    }
            )
        }

        val exc = shouldThrow<RejectedExecutionException> {
            dispatch.close()
        }
        exc.message!!.contains("timeout while waiting for coroutines finishing").shouldBeTrue()
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
            val result = dispatcher.compose { future { mySuspendFunction() } }.await()

            suspendFunctionInvoked.get().shouldBeTrue()
            result.shouldBe("suspend-function-result")
        }
    }

    fun createDispatcher(
            rateLimitRequestPerSecond: Int = 500,
            closingTimeout: Int = 5000,
            profiler: Profiler = NoopProfiler()) =
            SuspendableRateLimitedDispatcher(
                    DISPATCHER_NAME,
                    ConfigurableRateLimiter("rate-limiter-name", rateLimitRequestPerSecond),
                    profiler,
                    DynamicProperty.of(closingTimeout.toLong())
            )

    inner class TrackableDispatcher(
            profiler: Profiler = NoopProfiler()
    ) {

        private val dispatcher = createDispatcher(profiler = profiler)
        val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()
        val isSubmittedTaskFinished = HashMap<Int, AtomicBoolean>()

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

        /**
         * We want to be sure, that tasks from first range will be submitted before tasks from second range
         */
        suspend fun submitTasks(range1: IntRange, range2: IntRange) {
            submitTasks(range1)
            delay(200)
            submitTasks(range2)
        }

        private suspend fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, completedFuture(taskIndex), 0)

        suspend fun submitTask(taskIndex: Int, sleepTo: Long) = submitTask(taskIndex, CompletableFuture(), sleepTo)

        private suspend fun submitTask(taskIndex: Int, future: CompletableFuture<Any?>, sleepTo: Long) {
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)
            isSubmittedTaskFinished[taskIndex] = AtomicBoolean(false)

            scope.async {
                dispatcher.compose {
                    logger.info { "Setting isSubmittedTaskInvoked with index $taskIndex to true" }
                    isSubmittedTaskInvoked[taskIndex]!!.set(true)
                    if (sleepTo > 0) {
                        sleep(Random.nextLong(500, sleepTo))    //long operation imitation with blocking
                    }
                    logger.info { "Setting isSubmittedTaskFinished with index $taskIndex to true" }
                    isSubmittedTaskFinished[taskIndex]!!.set(true)
                    future
                }
            }

        }

        fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }
        fun isSubmittedTaskFinished(range: IntRange) = range.all { isSubmittedTaskFinished[it]!!.get() }

        fun completeAllAndClose() {
            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

    }

    private fun ProfilerReport.getMetric(metric: String): Long {
        return indicators.mapKeys { it.key.name }["$DISPATCHER_METRICS_PREFIX.$metric"]!!
    }

}