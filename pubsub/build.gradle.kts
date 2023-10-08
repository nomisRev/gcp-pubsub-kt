plugins {
  id(libs.plugins.kotlin.jvm.get().pluginId)
  id(libs.plugins.dokka.get().pluginId)
  id(libs.plugins.kover.get().pluginId)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
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
  implementation(kotlin("stdlib"))
  api(libs.coroutines)
  api(libs.pubsub)
  api(projects.googleCommonApi)

  testImplementation(kotlin("test"))
  testImplementation(projects.gcpPubsubTest)
}
