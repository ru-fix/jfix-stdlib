import org.gradle.kotlin.dsl.*

plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.kotlin_jdk8)
    compile(Libs.kotlin_stdlib)
    compile(Libs.kotlin_reflect)

    testCompile(Libs.kotlin_jdk8)
    testCompile(Libs.kotlin_stdlib)
    testCompile(Libs.kotlin_reflect)

    testImplementation(Libs.junit_api)
    testImplementation(Libs.mockito)
    testRuntimeOnly(Libs.junit_engine)
}
