@Suppress("DSL_SCOPE_VIOLATION") plugins {
  id(libs.plugins.kotlin.jvm.get().pluginId)
  id(libs.plugins.kotest.multiplatform.get().pluginId)
  id(libs.plugins.dokka.get().pluginId)
  id(libs.plugins.kover.get().pluginId)
  id(libs.plugins.spotless.get().pluginId)
  id(libs.plugins.knit.get().pluginId)
}

repositories {
  mavenCentral()
}

configure<JavaPluginExtension> {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
  }
}

spotless {
  kotlin {
    targetExclude("**/build/**")
    ktfmt().googleStyle()
  }
}

tasks.withType<Test> {
  useJUnitPlatform()
}

dependencies {
  api("org.junit.jupiter:junit-jupiter-api:5.10.0")
  api(libs.pubsub)
  implementation(libs.testcontainer.gcloud)
}
