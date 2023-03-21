package ru.fix.stdlib.concurrency.settings

import java.time.Duration
import java.time.temporal.ChronoUnit

data class ProfiledThreadPoolSettings(

    /**
     * see [java.util.concurrent.ThreadPoolExecutor.setCorePoolSize]
     */
    val corePoolSize: Int,

    /**
     * see [java.util.concurrent.ThreadPoolExecutor.setMaximumPoolSize]
     */
    val maxPoolSize: Int,

    /**
     * see [java.util.concurrent.ThreadPoolExecutor.setKeepAliveTime]
     */
    val keepAliveTime: Duration = THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION,

    /**
     * see [java.util.concurrent.ThreadPoolExecutor.allowCoreThreadTimeOut]
     */
    val allowCoreThreadTimeout: Boolean = false
) {
    companion object {

        @JvmField
        val THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION: Duration = Duration.of(60, ChronoUnit.SECONDS)

        @JvmStatic
        fun defaultSingleThreadPoolSettings() = ProfiledThreadPoolSettings(
            corePoolSize = 1,
            maxPoolSize = 1
        )

        @JvmStatic
        fun defaultProfiledThreadPoolSettings(corePoolSize: Int, maxPoolSize: Int) = ProfiledThreadPoolSettings(
            corePoolSize = corePoolSize,
            maxPoolSize = maxPoolSize
        )

        @JvmStatic
        fun legacyProfiledThreadPoolSettings(maxPoolSize: Int) = ProfiledThreadPoolSettings(
            corePoolSize = maxPoolSize,
            maxPoolSize = maxPoolSize,
            keepAliveTime = THREAD_IDLE_TIMEOUT_BEFORE_TERMINATION,
            allowCoreThreadTimeout = true
        )
    }
}
