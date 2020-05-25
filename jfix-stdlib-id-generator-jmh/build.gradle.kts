plugins {
    java
    kotlin("jvm")
    id("me.champeau.gradle.jmh")
}


jmh{
    warmupIterations = 2
    fork = 2
    threads = 8
    duplicateClassesStrategy  = DuplicatesStrategy.WARN
}


dependencies {

    implementation(project(":jfix-stdlib-id-generator"))

    /**
     * Runtime
     */
    implementation(Libs.slf4j_api)
    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.kotlin_reflect)
    implementation(Libs.slf4j_simple)
    implementation(Libs.jmh)
    implementation(Libs.jmh_generator_ann)
    implementation(Libs.jmh_generator_bytecode)
}


