import org.gradle.kotlin.dsl.*

plugins {
    java
}

dependencies {
    testImplementation(Libs.junit_api)
    testImplementation(Libs.mockito)
    testRuntimeOnly(Libs.junit_engine)
}
