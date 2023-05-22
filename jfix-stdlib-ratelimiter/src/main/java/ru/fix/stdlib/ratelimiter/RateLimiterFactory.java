package ru.fix.stdlib.ratelimiter;


import ru.fix.dynamic.property.api.DynamicProperty;

public interface RateLimiterFactory {
    RateLimiter createLimiter(String limiterId, DynamicProperty<Double> ratePropertyHolder) throws Exception;

    RateLimiterKt createLimiterKt(String limiterId, DynamicProperty<Double> ratePropertyHolder) throws Exception;
}