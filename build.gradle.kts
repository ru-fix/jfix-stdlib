import de.marcphilipp.gradle.nexus.NexusPublishExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.time.Duration
import java.time.temporal.ChronoUnit
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty

buildscript {
    repositories {
        mavenCentral()
        maven {
            url = uri("https://plugins.gradle.org/m2/")
        }
    }
    dependencies {
        classpath(Libs.gradle_release_plugin)
        classpath(Libs.dokka_gradle_plugin)
        classpath(Libs.gradle_kotlin_stdlib)
        classpath(Libs.gradle_kotlin_jdk8)
    }
}

plugins {
    kotlin("jvm") version Vers.kotlin apply false
    signing
    `maven-publish`
    id(Libs.nexus_publish_plugin) version "0.4.0" apply false
    id(Libs.nexus_staging_plugin) version "0.21.2"
    id(Libs.jmh_gradle_plugin) version Vers.jmh_plugin apply false
}

/**
 * Project configuration by properties and environment
 */
fun envConfig() = object : ReadOnlyProperty<Any?, String?> {
    override fun getValue(thisRef: Any?, property: KProperty<*>): String? =
            if (ext.has(property.name)) {
                ext[property.name] as? String
            } else {
                System.getenv(property.name)
            }
}

val repositoryUser by envConfig()
val repositoryPassword by envConfig()
val repositoryUrl by envConfig()
val signingKeyId by envConfig()
val signingPassword by envConfig()
val signingSecretKeyRingFile by envConfig()

nexusStaging {
    packageGroup = "ru.fix"
    username = "$repositoryUser"
    password = "$repositoryPassword"
    numberOfRetries = 50
    delayBetweenRetriesInMillis = 3_000
}

apply {
    plugin("ru.fix.gradle.release")
}

subprojects {
    group = "ru.fix"

    apply {
        plugin("maven-publish")
        plugin("signing")
        plugin("java")
        plugin(Libs.dokka_gradle_plugin_id)
        plugin(Libs.nexus_publish_plugin)
    }

    repositories {
        mavenCentral()
        mavenLocal()
    }

    val sourcesJar by tasks.creating(Jar::class) {
        archiveClassifier.set("sources")
        from("src/main/java")
        from("src/main/kotlin")
    }

    val dokkaTask = tasks.getByName<DokkaTask>("dokkaJavadoc") {
        outputDirectory.set(buildDir.resolve("dokka"))
    }

    val dokkaJar by tasks.creating(Jar::class) {
        archiveClassifier.set("javadoc")

        from(dokkaTask.outputDirectory)
        dependsOn(dokkaTask)
    }

    configure<NexusPublishExtension> {
        repositories {
            sonatype {
                username.set("$repositoryUser")
                password.set("$repositoryPassword")
                useStaging.set(true)
                stagingProfileId.set("1f0730098fd259")
            }
        }
        clientTimeout.set(Duration.of(4, ChronoUnit.MINUTES))
        connectTimeout.set(Duration.of(4, ChronoUnit.MINUTES))
    }


    project.afterEvaluate {
        publishing {

            publications {
                //Internal repository setup
                repositories {
                    maven {
                        url = uri("$repositoryUrl")
                        if (url.scheme.startsWith("http", true)) {
                            credentials {
                                username = "$repositoryUser"
                                password = "$repositoryPassword"
                            }
                        }
                    }
                }

                create<MavenPublication>("maven") {
                    from(components["java"])

                    artifact(sourcesJar)
                    artifact(dokkaJar)

                    pom {
                        name.set("${project.group}:${project.name}")
                        description.set("https://github.com/ru-fix/")
                        url.set("https://github.com/ru-fix/${rootProject.name}")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }
                        developers {
                            developer {
                                id.set("JFix Team")
                                name.set("JFix Team")
                                url.set("https://github.com/ru-fix/")
                            }
                        }
                        scm {
                            url.set("https://github.com/ru-fix/${rootProject.name}")
                            connection.set("https://github.com/ru-fix/${rootProject.name}.git")
                            developerConnection.set("https://github.com/ru-fix/${rootProject.name}.git")
                        }
                    }
                }
            }//publications
        }//publishing
    }//afterEvaluate

    configure<SigningExtension> {
        if (!signingKeyId.isNullOrEmpty()) {
            project.ext["signing.keyId"] = signingKeyId
            project.ext["signing.password"] = signingPassword
            project.ext["signing.secretKeyRingFile"] = signingSecretKeyRingFile
            logger.info("Signing key id provided. Sign artifacts for $project.")
            isRequired = true
        } else {
            logger.info("${project.name}: Signing key not provided. Disable signing for  $project.")
            isRequired = false
        }
        sign(publishing.publications)
    }

    tasks {
        withType<JavaCompile> {
            sourceCompatibility = JavaVersion.VERSION_1_8.toString()
            targetCompatibility = JavaVersion.VERSION_1_8.toString()
        }

        withType<KotlinCompile> {
            kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
        }
        withType<Test> {
            useJUnitPlatform()

            maxParallelForks = 10

            testLogging {
                events(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.SKIPPED)
                showStandardStreams = true
                exceptionFormat = TestExceptionFormat.FULL
            }
        }
    }
}
