plugins {
  base
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.dokka)
  alias(libs.plugins.kover)
  alias(libs.plugins.publish) apply false
  alias(libs.plugins.knit)
  alias(libs.plugins.spotless)
}
