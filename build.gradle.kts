plugins {
  base
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotest.multiplatform)
  alias(libs.plugins.dokka)
  alias(libs.plugins.kover)
  alias(libs.plugins.publish)
  alias(libs.plugins.knit)
  alias(libs.plugins.spotless)
}

repositories {
  mavenCentral()
}
