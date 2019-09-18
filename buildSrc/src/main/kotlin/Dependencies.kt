object Vers {
    const val kotlin = "1.3.50"
    const val kotlin_coroutines = "1.2.2"
    const val sl4j = "1.7.25"
    const val dokkav = "0.9.18"
    const val gradle_release_plugin = "1.3.9"
    const val junit = "5.5.2"
    const val hamkrest = "1.4.2.2"

    const val resilience4j = "0.13.0"
    const val aggregatingProfiler = "1.5.16"
    const val dynamic_property = "1.1.1"

    const val jmh = "1.21"
}

object Libs {
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"
    const val kotlinx_coroutines_core = "org.jetbrains.kotlinx:kotlinx-coroutines-core:${Vers.kotlin_coroutines}"

    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"

    const val slf4j_api = "org.slf4j:slf4j-api:${Vers.sl4j}"
    const val slf4j_simple = "org.slf4j:slf4j-simple:${Vers.sl4j}"

    const val mockito = "org.mockito:mockito-all:1.10.19"
    const val kotlin_logging = "io.github.microutils:kotlin-logging:1.4.9"

    const val junit_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    const val junit_parametri = "org.junit.jupiter:junit-jupiter-params:${Vers.junit}"
    const val junit_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    const val hamkrest = "com.natpryce:hamkrest:${Vers.hamkrest}"
    const val hamcrest = "org.hamcrest:hamcrest-all:1.3"


    const val resilience4j_rate_limiter = "io.github.resilience4j:resilience4j-ratelimiter:${Vers.resilience4j}"
    const val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregatingProfiler}"
    const val dynamic_property = "ru.fix:dynamic-property-api:${Vers.dynamic_property}"

    const val jmh_gradle_plugin = "me.champeau.gradle:jmh-gradle-plugin:0.5.0-rc-2"
    const val jmh = "org.openjdk.jmh:jmh-core:${Vers.jmh}"
    const val jmh_generator_ann = "org.openjdk.jmh:jmh-generator-annprocess:${Vers.jmh}"
    const val jmh_generator_bytecode = "org.openjdk.jmh:jmh-generator-bytecode:${Vers.jmh}"

    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    const val awaitility = "org.awaitility:awaitility:4.0.1"
}
