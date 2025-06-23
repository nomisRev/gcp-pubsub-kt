plugins {
  id(libs.plugins.kotlin.jvm.get().pluginId)
  id(libs.plugins.dokka.get().pluginId)
  id(libs.plugins.kover.get().pluginId)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
  kotlin("plugin.serialization") version "2.2.0"
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

kotlin { explicitApi() }

dependencies {
  api(kotlin("stdlib"))
  api(projects.gcpPubsub)
  api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

  testImplementation(kotlin("test"))
  testImplementation(projects.gcpPubsubTest)
}
