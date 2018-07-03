package ru.fix.stdlib.ratelimiter

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import ru.fix.commons.profiler.NoopProfiler
import ru.fix.commons.profiler.ProfiledCall
import ru.fix.commons.profiler.impl.SimpleProfiler
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.stream.Collectors

class RateLimitedDispatcherTest {

    private lateinit var dispatcher: RateLimitedDispatcher

    @BeforeEach
    fun before() {
        val limiter = ConfigurableRateLimiter("test-rate-limiter", RATE_LIMIT)
        dispatcher = RateLimitedDispatcher("test-rate-limiter-dispatcher", limiter, NoopProfiler())
    }

    @AfterEach
    fun after() {
        dispatcher.close()
    }

    @Test
    fun testSubmitIncrementThroughput() {
        testThroughput(BiFunction { call, counter -> this.submitIncrement(call, counter) })
    }

    @Test
    fun testComposeIncrementThroughput() {
        testThroughput(BiFunction { call, counter -> this.composeIncrement(call, counter) })
    }

    private fun testThroughput(biFunction: BiFunction<ProfiledCall, AtomicInteger, CompletableFuture<Int>>) {
        val counter = AtomicInteger(0)
        val profiler = SimpleProfiler()
        val profilerReporter = profiler.createReporter()

        profilerReporter.buildReportAndReset()
        val profiledCall = profiler.profiledCall(RateLimitedDispatcher::class.toString())

        val features = ArrayList<CompletableFuture<Int>>()
        for (i in 0 until ITERATIONS) {
            features.add(biFunction.apply(profiledCall, counter))
        }

        CompletableFuture.allOf(*features.toTypedArray()).join()
        val report = profilerReporter.buildReportAndReset().profilerCallReports[0]

        val results = features
                .stream()
                .map<Int>({ it.join() })
                .collect(Collectors.toList())

        for (i in 0 until ITERATIONS) {
            assertTrue(results.contains(i))
        }

        LOGGER.info("Current throughput " + report.callsThroughputAvg)

        assertTrue(report.callsThroughputAvg <= RATE_LIMIT * 1.25)

        assertEquals(ITERATIONS, counter.get())
    }

    private fun submitIncrement(call: ProfiledCall, counter: AtomicInteger): CompletableFuture<Int> {
        return dispatcher.submit {
            call.start()
            val result = counter.getAndIncrement()
            call.stop()
            result
        }
    }

    private fun composeIncrement(call: ProfiledCall, counter: AtomicInteger): CompletableFuture<Int> {
        return dispatcher.compose {
            call.start()
            val future = CompletableFuture.supplyAsync<Int>({ counter.getAndIncrement() })
            call.stop()
            future
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(RateLimitedDispatcherTest::class.java)
        private val RATE_LIMIT = 5701
        private val ITERATIONS = 5 * RATE_LIMIT
    }

}
