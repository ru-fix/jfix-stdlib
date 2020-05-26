plugins {
    java
    kotlin("jvm")
}

dependencies {
    api(Libs.slf4j_api)

    testImplementation(Libs.junit_api)
    testRuntimeOnly(Libs.junit_engine)
    testRuntimeOnly(Libs.slf4j_simple)
}
