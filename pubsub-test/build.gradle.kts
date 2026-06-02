import java.net.URI
import java.net.URL

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

tasks.withType<Test> {
  useJUnitPlatform()
}

kotlin { explicitApi() }

dependencies {
  api(projects.gcpPubsubKtor)
  api(libs.ktor.server)
  api(libs.testcontainers.gcloud)
  api(libs.testcontainers)
}

dokka {
  dokkaSourceSets.configureEach {
    includes.from("README.MD")
    perPackageOption {
      matchingRegex.set(".*\\.internal.*")
      suppress.set(true)
    }
    externalDocumentationLinks.configureEach {
      url("https://kotlinlang.org/api/kotlinx.coroutines/")
    }
    skipDeprecated.set(true)
    reportUndocumented.set(false)

    sourceLink {
      localDirectory.set(file("src/main/kotlin"))
      remoteUrl.set(URI("https://github.com/nomisRev/gcp-pubsub-kt/tree/main/pubsub-test/src"))
      remoteLineSuffix.set("#L")
    }
  }
}
