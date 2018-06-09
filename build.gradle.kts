import org.gradle.kotlin.dsl.kotlin
import org.gradle.kotlin.dsl.maven
import org.gradle.kotlin.dsl.repositories
import java.net.URI
import ru.fix.gradle.release.plugin.ReleaseExtension
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.artifacts.dsl.*
import org.gradle.kotlin.dsl.extra
import org.gradle.api.publication.maven.internal.action.MavenInstallAction
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.gradle.internal.authentication.DefaultBasicAuthentication
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.version
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import kotlin.properties.Delegates
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty

val groupId = "ru.fix"

buildscript {
    repositories {
        jcenter()
        gradlePluginPortal()
        mavenCentral()
    }
    dependencies {
        classpath(Libs.gradleReleasePlugin)
        classpath(Libs.dokkaGradlePlugin)
        classpath(Libs.kotlin_stdlib)
        classpath(Libs.kotlin_jdk8)
        classpath(Libs.kotlin_reflect)
    }
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

repositories {
    jcenter()
    gradlePluginPortal()
    mavenCentral()
}

plugins {
    kotlin("jvm") version "${Vers.kotlin}" apply false
    signing
}

apply {
    plugin("ru.fix.gradle.release")
}

subprojects {
    group = "ru.fix"

    apply {
        plugin("maven")
        plugin("signing")
        plugin("java")
    }

    repositories {
        mavenCentral()
        jcenter()
    }


    val sourcesJar by tasks.creating(Jar::class) {
        classifier = "sources"
        from("src/main/java")
        from("src/main/kotlin")
    }

    val javadocJar by tasks.creating(Jar::class) {
        classifier = "javadoc"

        val javadoc = tasks.getByPath("javadoc") as Javadoc
        from(javadoc.destinationDir)

        dependsOn(tasks.getByName("javadoc"))
    }

    artifacts {
        add("archives", sourcesJar)
        add("archives", javadocJar)
    }

    configure<SigningExtension> {

        if (!signingKeyId.isNullOrEmpty()) {
            ext["signing.keyId"] = signingKeyId
            ext["signing.password"] = signingPassword
            ext["signing.secretKeyRingFile"] = signingSecretKeyRingFile
            isRequired = true
        } else {
            logger.warn("${project.name}: Signing key not provided. Disable signing.")
            isRequired = false
        }

        sign(configurations.archives)
    }

    tasks {

        "uploadArchives"(Upload::class) {

            dependsOn(javadocJar, sourcesJar)

            repositories {
                withConvention(MavenRepositoryHandlerConvention::class) {
                    mavenDeployer {

                        withGroovyBuilder {
                            //Sign pom.xml file
                            "beforeDeployment" {
                                signing.signPom(delegate as MavenDeployment)
                            }

                            "repository"(
                                    "url" to URI("$repositoryUrl")) {
                                "authentication"(
                                        "userName" to "$repositoryUser",
                                        "password" to "$repositoryPassword"
                                )
                            }
                        }

                        pom.project {
                            withGroovyBuilder {
                                "artifactId"("${project.name}")
                                "groupId"("$groupId")
                                "version"("$version")

                                "name"("${groupId}:${project.name}")
                                "description"("Commons Profiler provide basic API" +
                                        " for application metrics measurement.")

                                "url"("https://github.com/ru-fix/jfix-stdlib")

                                "licenses" {
                                    "license" {
                                        "name"("The Apache License, Version 2.0")
                                        "url"("http://www.apache.org/licenses/LICENSE-2.0.txt")
                                    }
                                }

                                "developers" {
                                    "developer"{
                                        "id"("swarmshine")
                                        "name"("Kamil Asfandiyarov")
                                        "url"("https://github.com/swarmshine")
                                    }
                                }
                                "scm" {
                                    "url"("https://github.com/ru-fix/jfix-stdlib")
                                    "connection"("https://github.com/ru-fix/jfix-stdlib.git")
                                    "developerConnection"("https://github.com/ru-fix/jfix-stdlib.git")
                                }
                            }
                        }
                    }
                }
            }
        }

        withType<KotlinCompile> {
            kotlinOptions.jvmTarget = "1.8"
        }

        withType<Test> {
            useJUnitPlatform()

            testLogging {
                events(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.SKIPPED)
                showStandardStreams = true
            }
        }
    }
}