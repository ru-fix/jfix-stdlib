plugins {
    java
    kotlin("jvm")
    id(Libs.jmh_gradle_plugin) apply true
}


jmh {
    warmupIterations.set(2)
    fork.set(2)
    threads.set(8)
    duplicateClassesStrategy.set(DuplicatesStrategy.WARN)
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
