plugins {
    java
    kotlin("jvm")
}

dependencies {

    api(Libs.resilience4j_rate_limiter)
    api(Libs.dynamic_property)
    api(Libs.aggregating_profiler)

    testImplementation(Libs.kotlin_jdk8)
    testImplementation(Libs.kotlin_stdlib)
    testImplementation(Libs.kotlin_reflect)

    testImplementation(Libs.hamcrest)
    testImplementation(Libs.mockito)

    testImplementation(Libs.junit_api)
    testImplementation(Libs.awaitility)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.slf4j_simple)
    testCompile(Libs.kotlin_logging)
}