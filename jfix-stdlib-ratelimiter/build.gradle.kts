plugins {
    java
    kotlin("jvm")
}

dependencies {

    api(Libs.resilience4j_rate_limiter)
    api(Libs.dynamic_property)
    api(Libs.aggregating_profiler)

    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.kotlin_reflect)
    implementation(Libs.kotlinx_coroutines_core)
    implementation(Libs.kotlinx_coroutines_jdk8)

    testImplementation(Libs.hamcrest)
    testImplementation(Libs.mockito)

    testImplementation(Libs.junit_api)
    testImplementation(Libs.awaitility)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.slf4j_simple)
    testImplementation(Libs.kotlin_logging)
    testImplementation(Libs.kotest_assertions)

}