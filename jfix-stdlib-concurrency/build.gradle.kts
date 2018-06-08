import org.gradle.kotlin.dsl.*


plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.dynamicProperty)
    compile(Libs.aggregatingProfiler)

    testCompile(Libs.kotlin_jdk8)
    testCompile(Libs.kotlin_stdlib)
    testCompile(Libs.kotlin_reflect)

    testImplementation(Libs.junit_api)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.slf4j_simple)


}


