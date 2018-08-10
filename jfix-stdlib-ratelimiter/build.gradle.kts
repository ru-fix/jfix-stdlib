import org.gradle.kotlin.dsl.*


plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.resilience4jRatelimiter)
    compile(Libs.dynamicProperty)
    compile(Libs.aggregatingProfiler)

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