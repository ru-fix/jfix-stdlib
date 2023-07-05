package ru.fix.stdlib.ratelimiter

import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.*
import java.time.Duration
import java.util.concurrent.*

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RateLimitedDispatcherProviderTest {

    @Test
    fun `WHEN swithcing isSuspendableFlag THEN provider switches dispatcher realisation`() {
        val rateLimitedDispatcherProvider = createDispatcherProvider()
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
        rateLimitedDispatcherProvider.switchDispatcherProvider(false)
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<RateLimitedDispatcher>()
        rateLimitedDispatcherProvider.switchDispatcherProvider(true)
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
    }

    private fun createDispatcherProvider(): RateLimitedDispatcherProvider {
        return RateLimitedDispatcherProviderImpl(
            rateLimiterFactory = RateLimiterTestFactory(),
            rateLimiterSettings = DynamicProperty.of(
                RateLimiterSettings(
                    limitPerSec = 500.0,
                    closeTimeout = Duration.ofSeconds(5000),
                    isSuspendable = true
                )
            ),
            profiler = NoopProfiler(),
            suspendableWaitingScopeContext = Dispatchers.Default,
            limiterId = "dispatcher-name",
            window = DynamicProperty.of(0)
        )
    }

    /**
     * Change provider's dispatcher flag
     */
    private fun RateLimitedDispatcherProvider.switchDispatcherProvider(isSuspendable: Boolean) {
        this::class.java.declaredMethods
            .find {
                it.name == "updateDispatcher"
            }!!
            .apply { isAccessible = true }
            .invoke(this, isSuspendable)
    }

}
