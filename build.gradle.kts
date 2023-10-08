import org.jetbrains.dokka.gradle.DokkaTaskPartial
import org.jetbrains.kotlin.gradle.dsl.KotlinProjectExtension

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

subprojects {
  group = "io.github.nomisrev"

  this@subprojects.tasks.withType<DokkaTaskPartial>().configureEach {
    this@subprojects.extensions.findByType<KotlinProjectExtension>()?.sourceSets?.forEach { kotlinSourceSet ->
      dokkaSourceSets.named(kotlinSourceSet.name) {
        includes.from("README.MD")
        perPackageOption {
          matchingRegex.set(".*\\.internal.*")
          suppress.set(true)
        }
        externalDocumentationLink("https://kotlinlang.org/api/kotlinx.coroutines/")
        skipDeprecated.set(true)
        reportUndocumented.set(false)
        val baseUrl: String = checkNotNull(properties["pom.smc.url"]?.toString())

        kotlinSourceSet.kotlin.srcDirs.filter { it.exists() }.forEach { srcDir ->
          sourceLink {
            localDirectory.set(srcDir)
            remoteUrl.set(uri("$baseUrl/blob/main/${srcDir.relativeTo(rootProject.rootDir)}").toURL())
            remoteLineSuffix.set("#L")
          }
        }
      }
    }
  }

  tasks.withType<AbstractPublishToMaven> {
    dependsOn(tasks.withType<Sign>())
  }
}
