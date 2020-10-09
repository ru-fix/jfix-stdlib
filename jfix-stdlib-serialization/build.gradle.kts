plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.jackson_annotations)

    testImplementation(Libs.junit_jupiter)
    testImplementation(Libs.kotest)
    testImplementation(Libs.jackson)
}