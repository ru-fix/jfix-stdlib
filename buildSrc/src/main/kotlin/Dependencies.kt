object Vers {
    const val kotlin = "1.5.31"
    const val gradle_kotlin = "1.5.21"
    const val kotlin_coroutines = "1.3.8"
    const val sl4j = "1.7.25"
    const val dokkav = "1.4.30"
    const val gradle_release_plugin = "1.4.3"
    const val junit = "5.5.2"
    const val hamkrest = "1.4.2.2"

    const val resilience4j = "0.13.0"
    const val aggregating_profiler = "1.5.16"
    const val dynamic_property = "2.0.4"

    const val jackson = "2.11.3"

    const val jmh = "1.21"

    const val log4j = "2.12.0"

    const val kotest = "4.2.6"
}

object Libs {
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"
    const val kotlinx_coroutines_core = "org.jetbrains.kotlinx:kotlinx-coroutines-core:${Vers.kotlin_coroutines}"
    const val kotlinx_coroutines_jdk8 = "org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:${Vers.kotlin_coroutines}"

    const val gradle_kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.gradle_kotlin}"
    const val gradle_kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.gradle_kotlin}"

    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"

    const val slf4j_api = "org.slf4j:slf4j-api:${Vers.sl4j}"
    const val slf4j_simple = "org.slf4j:slf4j-simple:${Vers.sl4j}"

    val log4j_api = "org.apache.logging.log4j:log4j-api:${Vers.log4j}"
    val log4j_core = "org.apache.logging.log4j:log4j-core:${Vers.log4j}"
    val log4j_kotlin = "org.apache.logging.log4j:log4j-api-kotlin:1.0.0"
    val slf4j_over_log4j = "org.apache.logging.log4j:log4j-slf4j-impl:${Vers.log4j}"

    const val mockito = "org.mockito:mockito-all:1.10.19"
    const val kotlin_logging = "io.github.microutils:kotlin-logging:1.4.9"

    const val junit_jupiter = "org.junit.jupiter:junit-jupiter:${Vers.junit}"
    const val junit_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    const val junit_params = "org.junit.jupiter:junit-jupiter-params:${Vers.junit}"
    const val junit_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    const val hamkrest = "com.natpryce:hamkrest:${Vers.hamkrest}"
    const val hamcrest = "org.hamcrest:hamcrest-all:1.3"
    const val kotest = "io.kotest:kotest-runner-junit5-jvm:${Vers.kotest}"

    const val jackson = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson}"
    const val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"

    const val resilience4j_rate_limiter = "io.github.resilience4j:resilience4j-ratelimiter:${Vers.resilience4j}"
    const val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregating_profiler}"
    const val dynamic_property = "ru.fix:dynamic-property-api:${Vers.dynamic_property}"

    const val jmh_gradle_plugin = "me.champeau.gradle:jmh-gradle-plugin:0.5.0-rc-2"
    const val jmh = "org.openjdk.jmh:jmh-core:${Vers.jmh}"
    const val jmh_generator_ann = "org.openjdk.jmh:jmh-generator-annprocess:${Vers.jmh}"
    const val jmh_generator_bytecode = "org.openjdk.jmh:jmh-generator-bytecode:${Vers.jmh}"

    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    const val awaitility = "org.awaitility:awaitility:4.0.1"
    const val kotest_assertions = "io.kotest:kotest-assertions-core:4.1.1"
}


enum class Projs {
    jfix_stdlib_concurrency,
    jfix_stdlib_files,
    jfix_stdlib_id_generator,
    jfix_stdlib_id_generator_jmh,
    jfix_stdlib_ratelimiter,
    jfix_stdlib_socket;

    val directory get() = this.name.replace('_', '-')
    val dependency get(): String = ":$directory"
}