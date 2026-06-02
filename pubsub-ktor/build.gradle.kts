import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.net.URI

plugins {
  id(libs.plugins.kotlin.jvm.get().pluginId)
  id(libs.plugins.dokka.get().pluginId)
  id(libs.plugins.kover.get().pluginId)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
}

spotless {
  kotlin {
    targetExclude("**/build/**")
    ktfmt().googleStyle()
  }
}

kotlin {
  jvmToolchain(21)
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_11)
  }
  explicitApi()
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(11))
  }
}

dependencies {
  api(projects.gcpPubsub)
  api(libs.ktor.server)

  testImplementation(libs.ktor.test)
  testImplementation(projects.gcpPubsubTest)
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
      remoteUrl.set(URI("https://github.com/nomisRev/gcp-pubsub-kt/tree/main/pubsub-ktor/src"))
      remoteLineSuffix.set("#L")
    }
  }
}
