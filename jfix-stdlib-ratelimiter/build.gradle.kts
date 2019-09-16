plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.resilience4j_rate_limiter)
    compile(Libs.dynamic_property)
    compile(Libs.aggregating_profiler)

    testCompile(Libs.kotlin_jdk8)
    testCompile(Libs.kotlin_stdlib)
    testCompile(Libs.kotlin_reflect)

    testCompile(Libs.hamcrest)
    testCompile(Libs.mockito)

    testCompile(Libs.junit_api)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.slf4j_simple)
    testCompile(Libs.kotlin_logging)
}