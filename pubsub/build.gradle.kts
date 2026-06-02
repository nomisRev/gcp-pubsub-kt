import java.net.URI

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
      remoteUrl.set(URI("https://github.com/nomisRev/gcp-pubsub-kt/tree/main/pubsub/src"))
      remoteLineSuffix.set("#L")
    }
  }
}
