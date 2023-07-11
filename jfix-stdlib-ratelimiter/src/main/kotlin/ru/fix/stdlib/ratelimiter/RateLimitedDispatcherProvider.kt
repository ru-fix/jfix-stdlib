package ru.fix.stdlib.ratelimiter

interface RateLimitedDispatcherProvider {

    fun provideDispatcher(): RateLimitedDispatcherInterface

}
