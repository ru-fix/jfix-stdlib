plugins {
    java
    kotlin("jvm")
}

dependencies {

    api(Libs.dynamic_property)
    api(Libs.aggregating_profiler)

    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.kotlin_reflect)

    testImplementation(Libs.hamkrest)
    testImplementation(Libs.awaitility)
    testImplementation(Libs.junit_api)
    testImplementation(Libs.junit_parametri)
    testRuntimeOnly(Libs.junit_engine)
    testRuntimeOnly(Libs.slf4j_simple)


}


