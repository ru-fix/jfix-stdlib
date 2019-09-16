object Vers {
    val kotlin = "1.3.50"
    val kotlin_coroutines = "1.2.1"
    val sl4j = "1.7.25"
    val dokkav = "0.9.18"
    val gradle_release_plugin = "1.3.9"
    val junit = "5.2.0"
    val hamkrest = "1.4.2.2"

    val resilience4j = "0.13.0"
    val aggregatingProfiler = "1.5.16"
    val dynamicProperty = "1.0.5"

    val jmh = "1.21"
}

object Libs {
    val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"
    val kotlinx_coroutines_core = "org.jetbrains.kotlinx:kotlinx-coroutines-core:${Vers.kotlin_coroutines}"

    val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"

    val slf4j_api = "org.slf4j:slf4j-api:${Vers.sl4j}"
    val slf4j_simple = "org.slf4j:slf4j-simple:${Vers.sl4j}"

    val mockito = "org.mockito:mockito-all:1.10.19"
    val mockito_kotiln = "com.nhaarman:mockito-kotlin-kt1.1:1.5.0"
    val kotlin_logging = "io.github.microutils:kotlin-logging:1.4.9"

    val junit_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    val junit_parametri = "org.junit.jupiter:junit-jupiter-params:${Vers.junit}"
    val junit_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    val hamkrest = "com.natpryce:hamkrest:${Vers.hamkrest}"
    val hamcrest = "org.hamcrest:hamcrest-all:1.3"


    val resilience4j_rate_limiter = "io.github.resilience4j:resilience4j-ratelimiter:${Vers.resilience4j}"
    val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregatingProfiler}"
    val dynamic_property = "ru.fix:dynamic-property-api:${Vers.dynamicProperty}"

    val jmh_gradle_plugin = "me.champeau.gradle:jmh-gradle-plugin:0.5.0-rc-2"
    val jmh = "org.openjdk.jmh:jmh-core:${Vers.jmh}"
    val jmh_generator_ann = "org.openjdk.jmh:jmh-generator-annprocess:${Vers.jmh}"
    val jmh_generator_bytecode = "org.openjdk.jmh:jmh-generator-bytecode:${Vers.jmh}"

    val nexus_staging_plugin = "io.codearte.nexus-staging"
    val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    val awaitility = "org.awaitility:awaitility:4.0.1"
}
