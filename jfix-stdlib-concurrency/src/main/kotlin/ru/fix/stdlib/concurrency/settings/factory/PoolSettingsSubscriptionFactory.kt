package ru.fix.stdlib.concurrency.settings.factory

import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.dynamic.property.api.PropertySubscription
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings
import ru.fix.stdlib.concurrency.threads.ProfiledThreadPoolExecutor
import java.util.function.BiFunction

internal fun interface PoolSettingsSubscriptionFactory : BiFunction<
        ProfiledThreadPoolExecutor,
        DynamicProperty<ProfiledThreadPoolSettings>,
        PropertySubscription<ProfiledThreadPoolSettings>>
