plugins {
    java
    kotlin("jvm")
}

dependencies {

    api(Libs.dynamic_property)
    api(Libs.aggregating_profiler)
    api(Libs.kotlin_logging)

    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.kotlin_reflect)

    testImplementation(Libs.hamkrest)
    testImplementation(Libs.awaitility)
    testImplementation(Libs.junit_api)
    testImplementation(Libs.junit_params)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.log4j_core)
    testRuntimeOnly(Libs.slf4j_over_log4j)


}

tasks {
    withType<JavaCompile> {
        sourceCompatibility = JavaVersion.VERSION_1_8.toString()
        targetCompatibility = JavaVersion.VERSION_1_8.toString()
    }
}
