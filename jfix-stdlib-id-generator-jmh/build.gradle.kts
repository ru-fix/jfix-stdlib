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
    runtimeOnly(Libs.jmh)
    runtimeOnly(Libs.jmh_generator_bytecode)
    implementation(Libs.jmh_generator_ann)
}


