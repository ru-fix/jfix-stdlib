package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.longs.shouldBeInRange
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.throwable.shouldHaveCause
import io.kotest.matchers.throwable.shouldHaveCauseOfType
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import mu.KLogging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.*
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class SuspendableRateLimitedDispatcherTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val DISPATCHER_METRICS_PREFIX = "RateLimiterDispatcher.$DISPATCHER_NAME"
        const val RATE_PER_SECOND = 500
        const val ITERATIONS = 5 * RATE_PER_SECOND

        @JvmStatic
        fun trackableDispatcherSource() =
            listOf(
                with(AggregatingProfiler()) {
                    return@with Arguments.of(
                        SuspendableRateLimitedDispatcherTest().TrackableDispatcherWithCF(this),
                        this.createReporter()
                    )
                },
                with(AggregatingProfiler()) {
                    return@with Arguments.of(
                        SuspendableRateLimitedDispatcherTest().TrackableDispatcherWithSuspend(this),
                        this.createReporter()
                    )
                },
            )
    }

    val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    @Nested
    inner class CompletableFutureTest {
        @Test
        fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() {
            createDispatcher().use { dispatcher ->

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

                actualException.shouldHaveCause()
                actualException.message should {
                    it.shouldNotBeNull()
                    it.shouldContain(asyncOperationException.message!!)
                }
            }
        }

        @Test
        fun `series of operations with CompletableFuture`() {
            val report = `submit series of operations with CF`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
            )

            val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

            logger.info("Throughput $operationReport")
            operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
        }

        @Test
        fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution with CompletableFuture`() {
            val report = `submit series of operations with CF`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
            )

            report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

            report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

            report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

            logger.info { "Resulting profiler report: $report" }
        }

        @Test
        fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() {
            val report = `submit series of operations with CF`(500, 4000)

            logger.info { report }
            report.assertSoftly {
                it.getMetric("active_async_operations") shouldBe 0
            }
            return
        }

        /**
         * When dispatcher closingTimeout is enough for pending tasks to complete
         * such tasks will complete normally
         */
        @Test
        @Timeout(10, unit = TimeUnit.SECONDS)
        fun `on shutdown fast tasks with CompletableFuture complete normally`() {

            val futures: List<CompletableFuture<*>>
            createDispatcher(closingTimeout = 5_000).use { dispatcher ->
                futures = List(3) {
                    dispatcher.compose {
                        completedFuture(true)
                    }
                }
            }

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeFalse()
            }
        }

        @Test
        @Timeout(5, unit = TimeUnit.SECONDS)
        fun `on shutdown slow tasks with CompletableFuture complete exceptionally`() {

            val expectedException: Throwable
            createDispatcher(closingTimeout = 0).use { dispatcher ->

                for (i in 1..3) {
                    scope.launch {
                        dispatcher.compose {
                            GlobalScope.future {
                                delay(3_000)
                                true
                            }
                        }
                    }
                }

                expectedException = shouldThrow<RejectedExecutionException> {
                    dispatcher.close()
                }
            }
            expectedException.message!!.shouldContain("timeout while waiting for coroutines finishing")
        }

        @Test
        fun `task with CompletableFuture, submitted in closed dispatcher, is rejected with exception`() {
            val dispatcher = createDispatcher()
            dispatcher.close()

            val t = shouldThrowExactly<CompletionException> {
                dispatcher.compose {
                    completedFuture(true)
                }.join()
            }
            t.shouldHaveCauseOfType<RejectedExecutionException>()
            t.message should {
                it.shouldNotBeNull()
                it.shouldContain("TERMINATE")
            }
            return
        }

        @Test
        fun `invoke suspend function wrapped to completable future`() {
            createDispatcher().use { dispatcher ->

                val suspendFunctionInvoked = AtomicBoolean(false)

                suspend fun mySuspendFunction(): String {
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
        }

        private fun `submit series of operations with CF`(
            ratePerSecond: Int,
            iterations: Int
        ): ProfilerReport {

            val profiler = AggregatingProfiler()
            val report: ProfilerReport

            createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                profiler = profiler,
                context = Dispatchers.Unconfined
            ).use { dispatcher ->

                val counter = AtomicInteger(0)

                val profilerReporter = profiler.createReporter()
                val profiledCall = profiler.profiledCall("operation")

                val futures = List(iterations) {
                    dispatcher.compose {
                        profiledCall.profile<CompletableFuture<Int>> {
                            completedFuture(counter.incrementAndGet())
                        }
                    }
                }

                CompletableFuture.allOf(*futures.toTypedArray()).join()

                counter.get().shouldBe(iterations)
                futures.map { it.join() }.toSet().shouldContainAll((1..iterations).toList())

                report = profilerReporter.buildReportAndReset()
            }

            return report
        }
    }

    @Nested
    inner class SuspendTests {
        @Test
        fun `dispatch suspend operation with exceptional result, operation invoked and it's result returned`() = runBlocking {
            createDispatcher().use { dispatcher ->

                val asyncOperationException = Exception("some error")

                fun userAsyncOperation(): Any {
                    throw asyncOperationException
                }

                val actualException = shouldThrow<Exception> { dispatcher.decorateSuspend { userAsyncOperation() } }

                actualException.shouldHaveCause()
                actualException.message should {
                    it.shouldNotBeNull()
                    it.shouldContain(asyncOperationException.message!!)
                }
            }

        }

        @Test
        fun `series of operations`() = runBlocking {
            val RATE_PER_SECOND = 500
            val ITERATIONS = 5 * RATE_PER_SECOND

            val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
            )

            val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

            logger.info("Throughput $operationReport")
            operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
        }

        @Test
        fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution`() = runBlocking {

            val RATE_PER_SECOND = 500
            val ITERATIONS = 5 * RATE_PER_SECOND

            val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
            )

            report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

            report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

            report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

            logger.info { "Resulting profiler report: $report" }
        }

        @Test
        fun `WHEN many fast tasks completed THEN 'active_async_operations' is 0`() = runBlocking {
            val report = `submit series of operations`(500, 4000)

            logger.info { report }
            report.assertSoftly {
                it.getMetric("active_async_operations") shouldBe 0
            }
            return@runBlocking
        }

        /**
         * When dispatcher closingTimeout is enough for pending tasks to complete
         * such tasks will complete normally
         */
        @Test
        @Timeout(10, unit = TimeUnit.SECONDS)
        fun `on shutdown fast tasks complete normally`() = runBlocking {

            val results: List<Deferred<Boolean>>
            createDispatcher(closingTimeout = 5_000).use { dispatcher ->
                results = List(3) {
                    async {
                        dispatcher.decorateSuspend {
                            true
                        }
                    }
                }
                delay(10)
            }

            results.forEach { result ->
                result.await().shouldBeTrue()
            }
        }

        @Test
        @Timeout(5, unit = TimeUnit.SECONDS)
        fun `on shutdown slow tasks complete exceptionally`() {

            val expectedException: Throwable
            createDispatcher(closingTimeout = 0).use { dispatcher ->

                for (i in 1..3) {
                    scope.launch {
                        dispatcher.decorateSuspend {
                            delay(3_000)
                        }
                    }
                }

                expectedException = shouldThrow<RejectedExecutionException> {
                    dispatcher.close()
                }
            }
            expectedException.message!!.shouldContain("timeout while waiting for coroutines finishing")
        }

        @Test
        fun `task, submitted in closed dispatcher, is rejected with exception`() = runBlocking {
            val dispatcher = createDispatcher()
            dispatcher.close()

            val t = shouldThrowExactly<RejectedExecutionException> {
                dispatcher.decorateSuspend {
                    true
                }
            }
            t.message should {
                it.shouldNotBeNull()
                it.shouldContain("TERMINATE")
            }
            return@runBlocking
        }

        @Test
        fun `invoke suspend function`() {
            createDispatcher().use { dispatcher ->

                val suspendFunctionInvoked = AtomicBoolean(false)

                suspend fun mySuspendFunction(): String {
                    delay(500)
                    suspendFunctionInvoked.set(true)
                    return "suspend-function-result"
                }

                runBlocking {
                    val result = dispatcher.decorateSuspend { mySuspendFunction() }

                    suspendFunctionInvoked.get().shouldBeTrue()
                    result.shouldBe("suspend-function-result")
                }
            }
        }

        private suspend fun `submit series of operations`(
            ratePerSecond: Int,
            iterations: Int
        ): ProfilerReport {

            val profiler = AggregatingProfiler()
            val profilerReporter = profiler.createReporter()
            val fullReport: ProfilerReport

            createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                profiler = profiler
            ).use { dispatcher ->

                val counter = AtomicInteger(0)
                val deferreds: List<Deferred<Int>>
                val profilers: MutableList<Profiler> = mutableListOf()
                val reporters: MutableList<ProfilerReporter> = mutableListOf()
                for (i in 0 until iterations) {
                    profilers.add(i, AggregatingProfiler())
                    reporters.add(i, profilers[i].createReporter())
                }

                coroutineScope {
                    deferreds = List(iterations) {
                        async {
                            val profiledCall = profilers[it].profiledCall("operation")
                            dispatcher.decorateSuspend {
                                profiledCall.profile<Int> {
                                    counter.incrementAndGet()
                                }
                            }
                        }
                    }
                }

                val results = deferreds.map { it.await() }
                reporters.add(profilerReporter)
                fullReport = mergeProfileReports(reporters)

                counter.get().shouldBe(iterations)
                results.toSet().shouldContainAll((1..iterations).toList())
            }

            return fullReport
        }

        private fun mergeProfileReports(reporters: MutableList<ProfilerReporter>): ProfilerReport {
            val indicators: MutableMap<Identity, Long> = mutableMapOf()
            val profilerCallReports: MutableList<ProfiledCallReport> = mutableListOf()

            reporters.forEach { it ->
                val report = it.buildReportAndReset()

                report.indicators.forEach {
                    indicators.merge(it.key, it.value) { prev, new -> prev + new }
                }

                report.profilerCallReports.forEach { pcReport ->
                    val existReport = profilerCallReports.find { it.identity.name == pcReport.identity.name }
                    if (existReport == null) {
                        profilerCallReports.add(pcReport)
                    } else {
                        existReport.stopThroughputAvg = (existReport.stopThroughputAvg + pcReport.stopThroughputAvg)
                        existReport.stopSum = (existReport.stopSum + pcReport.stopSum)
                    }
                }
            }

            return ProfilerReport(indicators, profilerCallReports)
        }

    }

    @ParameterizedTest
    @MethodSource("trackableDispatcherSource")
    fun `indicators 'active_async_operations' adjusted according to number of queued and active operations`(
        trackableDispatcher: TrackableDispatcher,
        reporter: ProfilerReporter
    ) {

        trackableDispatcher.submitTasks(1..12, 1000)
        await().atMost(Duration.ofSeconds(2)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..10)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 12
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..10)
        }
        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcher.isSubmittedTaskInvoked(11..12)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBeInRange LongRange(0, 2)
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..12)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @ParameterizedTest
    @MethodSource("trackableDispatcherSource")
    fun `WHEN results arrived THEN indicators are correctly adjusted`(
        trackableDispatcher: TrackableDispatcher,
        reporter: ProfilerReporter
    ) {
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

    private fun createDispatcher(
            rateLimitRequestPerSecond: Int = 500,
            closingTimeout: Int = 5000,
            profiler: Profiler = NoopProfiler(),
            context: CoroutineContext = Dispatchers.IO
    ) =
            SuspendableRateLimitedDispatcher(
                    DISPATCHER_NAME,
                    ConfigurableRateLimiterKt("rate-limiter-name", rateLimitRequestPerSecond),
                    profiler,
                    DynamicProperty.of(closingTimeout.toLong()),
                    context
            )

    private fun ProfilerReport.getMetric(metric: String): Long {
        return indicators.mapKeys { it.key.name }["$DISPATCHER_METRICS_PREFIX.$metric"]!!
    }

    interface TrackableDispatcher {

        fun submitCompletedTasks(tasks: IntRange)

        fun submitTasks(tasks: IntRange, sleepTo: Long)

        fun isSubmittedTaskInvoked(range: IntRange): Boolean

        fun isSubmittedTaskFinished(range: IntRange): Boolean

        fun completeAllAndClose()

    }

    inner class TrackableDispatcherWithCF(
            profiler: Profiler = NoopProfiler()
    ) : TrackableDispatcher {

        private val dispatcher = createDispatcher(profiler = profiler)
        private val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()
        private val isSubmittedTaskFinished = HashMap<Int, AtomicBoolean>()

        override fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        override fun submitTasks(tasks: IntRange, sleepTo: Long) {
            for (task in tasks) {
                submitTask(task, sleepTo)
            }
        }

        override fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }
        override fun isSubmittedTaskFinished(range: IntRange) = range.all { isSubmittedTaskFinished[it]!!.get() }

        override fun completeAllAndClose() {
            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

        private fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, completedFuture(taskIndex), 0)

        private fun submitTask(taskIndex: Int, sleepTo: Long) = submitTask(taskIndex, CompletableFuture(), sleepTo)

        private fun submitTask(taskIndex: Int, future: CompletableFuture<Any?>, sleepTo: Long) {
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)
            isSubmittedTaskFinished[taskIndex] = AtomicBoolean(false)

            dispatcher.compose {
                logger.info { "Setting isSubmittedTaskInvoked with index $taskIndex to true" }
                isSubmittedTaskInvoked[taskIndex]!!.set(true)
                if (sleepTo > 0) {
                    sleep(sleepTo)    //long operation imitation with blocking
                    future.complete(taskIndex)
                }
                logger.info { "Setting isSubmittedTaskFinished with index $taskIndex to true" }
                isSubmittedTaskFinished[taskIndex]!!.set(true)
                logger.info { "Index has been set for index $taskIndex" }
                future
            }

        }

    }

    inner class TrackableDispatcherWithSuspend(
            profiler: Profiler = NoopProfiler()
    ) : TrackableDispatcher {

        private val dispatcher = createDispatcher(profiler = profiler)
        private val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()
        private val isSubmittedTaskFinished = HashMap<Int, AtomicBoolean>()

        override fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        override fun submitTasks(tasks: IntRange, sleepTo: Long) {
            for (task in tasks) {
                submitTask(task, sleepTo)
            }
        }

        override fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }
        override fun isSubmittedTaskFinished(range: IntRange) = range.all { isSubmittedTaskFinished[it]!!.get() }

        override fun completeAllAndClose() {
            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

        private fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, 0)

        private fun submitTask(taskIndex: Int, sleepTo: Long) {
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)
            isSubmittedTaskFinished[taskIndex] = AtomicBoolean(false)

            scope.launch {
                dispatcher.decorateSuspend {
                    logger.info { "Setting isSubmittedTaskInvoked with index $taskIndex to true" }
                    isSubmittedTaskInvoked[taskIndex]!!.set(true)
                    if (sleepTo > 0) {
                        withContext(Dispatchers.IO) {
                            sleep(sleepTo)    //long operation imitation with blocking
                        }
                    }
                    logger.info { "Setting isSubmittedTaskFinished with index $taskIndex to true" }
                    isSubmittedTaskFinished[taskIndex]!!.set(true)
                }
            }

        }

    }

}