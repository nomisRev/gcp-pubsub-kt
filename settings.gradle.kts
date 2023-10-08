enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

rootProject.name = "kotlin-gcp-pubsub"
include("google-common-api")
project(":google-common-api").projectDir = file("common-api")

include("gcp-pubsub")
project(":gcp-pubsub").projectDir = file("pubsub")

include("gcp-pubsub-kotlinx-serialization-json")
project(":gcp-pubsub-kotlinx-serialization-json").projectDir = file("pubsub-kotlinx-serialization-json")

include("gcp-pubsub-ktor")
project(":gcp-pubsub-ktor").projectDir = file("pubsub-ktor")

include("gcp-pubsub-ktor-kotlinx-serialization-json")
project(":gcp-pubsub-ktor-kotlinx-serialization-json").projectDir = file("pubsub-ktor-kotlinx-serialization-json")

include("gcp-pubsub-test")
project(":gcp-pubsub-test").projectDir = file("pubsub-test")
