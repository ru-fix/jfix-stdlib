package ru.fix.stdlib.ratelimiter

import ru.fix.dynamic.property.api.DynamicProperty

class RateLimiterTestFactory : RateLimiterFactory {

    companion object {
        const val RATE_PER_SECOND = 500
    }

    override fun createLimiter(limiterId: String?, ratePropertyHolder: DynamicProperty<Double>?): RateLimiter {
        return ConfigurableRateLimiter("rate-limiter-name", RATE_PER_SECOND)
    }

    override fun createLimiterKt(limiterId: String?, ratePropertyHolder: DynamicProperty<Double>?): RateLimiterKt {
        return ConfigurableRateLimiterKt("rate-limiter-name", RATE_PER_SECOND)
    }

}