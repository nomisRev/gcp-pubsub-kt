plugins {
  id(libs.plugins.kotlin.jvm.get().pluginId)
  id(libs.plugins.kotest.multiplatform.get().pluginId)
  id(libs.plugins.dokka.get().pluginId)
  id(libs.plugins.kover.get().pluginId)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
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

kotlin { explicitApi() }

dependencies {
  implementation(kotlin("stdlib"))
  api(libs.coroutines)
  api(libs.pubsub)
  api(projects.apiCore)

  testImplementation(libs.arrow.fx)
  testImplementation(libs.kotest.property)
  testImplementation(libs.kotest.assertions)
  testImplementation(libs.kotest.junit5)
  testImplementation(libs.testcontainer.gcloud)
}
