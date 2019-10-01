plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.log4j_kotlin)
    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.kotlin_reflect)

    implementation(project(Projs.jfix_stdlib_concurrency.dependency))

    testImplementation(Libs.junit_api)
    testImplementation(Libs.junit_params)
    testRuntimeOnly(Libs.junit_engine)

    testRuntimeOnly(Libs.log4j_core)
}
