plugins {
  kotlin("jvm") version "1.7.21"
  id("io.kotest.multiplatform") version "5.5.4"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

tasks.withType<Test> {
  useJUnitPlatform()
}

dependencies {
  implementation(kotlin("stdlib"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
  implementation("com.google.cloud:google-cloud-pubsub:1.122.1")

  testImplementation("io.arrow-kt:arrow-fx-coroutines:1.1.3")
  testImplementation("io.kotest:kotest-property:5.5.4")
  testImplementation("io.kotest:kotest-assertions-core:5.5.4")
  testImplementation("io.kotest:kotest-runner-junit5:5.5.4")
  testImplementation("org.testcontainers:gcloud:1.17.6")
}
