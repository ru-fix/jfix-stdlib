plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.slf4j_api)

    api(Libs.aggregating_profiler)
    api(Libs.dynamic_property)

    implementation(project(Projs.jfix_stdlib_concurrency.dependency))

    testImplementation(Libs.junit_jupiter)
    testRuntimeOnly(Libs.log4j_core)
    testRuntimeOnly(Libs.slf4j_over_log4j)
}
