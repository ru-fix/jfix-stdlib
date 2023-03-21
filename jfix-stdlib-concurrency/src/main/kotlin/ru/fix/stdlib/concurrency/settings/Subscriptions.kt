package ru.fix.stdlib.concurrency.settings

import ru.fix.stdlib.concurrency.settings.factory.PoolSettingsSubscriptionFactory
import java.util.concurrent.TimeUnit

internal class Subscriptions {

    internal companion object {

        /**
         * During first invocation of the listener PropertyListener#onPropertyChanged(Object, Object)
         * oldValue is going to be null.
         */
        @JvmStatic
        val defaultPoolSettingsSubscriptionFactory = PoolSettingsSubscriptionFactory { executor, poolSettings ->
            poolSettings.createSubscription().setAndCallListener { oldVal: ProfiledThreadPoolSettings?, newVal ->
                if (oldVal == null || (newVal.corePoolSize != oldVal.corePoolSize)) {
                    executor.corePoolSize = newVal.corePoolSize
                }

                if (oldVal == null || (newVal.maxPoolSize != oldVal.maxPoolSize)) {
                    executor.maximumPoolSize = newVal.maxPoolSize
                }

                if (oldVal == null || (newVal.keepAliveTime !== oldVal.keepAliveTime)) {
                    executor.setKeepAliveTime(newVal.keepAliveTime.seconds, TimeUnit.SECONDS)
                }

                if (oldVal == null || (newVal.allowCoreThreadTimeout != oldVal.allowCoreThreadTimeout)) {
                    executor.allowCoreThreadTimeOut(newVal.allowCoreThreadTimeout)
                }
            }
        }

        /**
         * During first invocation of the listener PropertyListener.onPropertyChanged(Object, Object)
         * oldValue is going to be null.
         */
        @JvmStatic
        val deprecatedPoolSettingsSubscriptionFactory = PoolSettingsSubscriptionFactory { executor, poolSettings ->
            poolSettings.createSubscription().setAndCallListener { oldVal: ProfiledThreadPoolSettings?, newVal ->
                if (oldVal == null || (newVal.maxPoolSize != oldVal.maxPoolSize)) {
                    executor.setMaxPoolSize(newVal.maxPoolSize)
                }
            }
        }
    }
}
