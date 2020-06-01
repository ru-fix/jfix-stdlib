plugins {
    java
    kotlin("jvm")
}

dependencies {

    // Should depend only on loggin api and JVM
    api(Libs.slf4j_api)

    testImplementation(Libs.kotlin_jdk8)
    testImplementation(Libs.kotlin_stdlib)
    testImplementation(Libs.kotlin_reflect)

    testImplementation(Libs.awaitility)
    testImplementation(Libs.junit_api)
    testImplementation(Libs.junit_params)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.log4j_core)
    testRuntimeOnly(Libs.slf4j_over_log4j)
}


