package ru.fix.stdlib.ratelimiter

import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.*
import java.time.Duration
import java.util.concurrent.*

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RateLimitedDispatcherProviderTest {

    @Test
    fun `WHEN swithcing isSuspendableFlag THEN provider switches dispatcher realisation`() {
        val settings: AtomicProperty<RateLimiterSettings> = AtomicProperty(createRateLimiterSettings(true))
        val rateLimitedDispatcherProvider = createDispatcherProvider(settings)

        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
        settings.set(createRateLimiterSettings(false))
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<RateLimitedDispatcher>()
        settings.set(createRateLimiterSettings(true))
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
    }

    private fun createRateLimiterSettings(isSuspendable: Boolean) =
        RateLimiterSettings(
            limitPerSec = 500.0,
            closeTimeout = Duration.ofSeconds(5000),
            isSuspendable = isSuspendable
        )

    private fun createDispatcherProvider(settings: DynamicProperty<RateLimiterSettings>): RateLimitedDispatcherProvider {
        return RateLimitedDispatcherProviderImpl(
            rateLimiterFactory = RateLimiterTestFactory(),
            rateLimiterSettings = settings,
            profiler = NoopProfiler(),
            suspendableWaitingScopeContext = Dispatchers.Default,
            limiterId = "dispatcher-name",
            window = DynamicProperty.of(0)
        )
    }

}
