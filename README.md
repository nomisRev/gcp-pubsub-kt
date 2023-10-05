# Kotlin GCP PubSub

Google Cloud PubSub made easy! Kotlin GCP PubSub offers idiomatic KotlinX & Ktor integration for GCP.

```kotlin
@Serializable
data class Event(val key: String, val message: String)

fun Application.pubSubApp() {
  pubSub(ProjectId("my-project")) {
    subscribe<Event>(SubscriptionId("my-subscription")) { (event, record) ->
      println("event.key: ${event.key}, event.message: ${event.message}")
      record.ack()
    }
  }

  routing {
    post("/publish/{key}/{message}") {
      val event = Event(call.parameters["key"]!!, call.parameters["message"]!!)
      
      pubSub()
        .publisher(ProjectId("my-project"))
        .publish(TopicId("my-topic"), event)
      
      call.respond(HttpStatusCode.Accepted)
    }
  }
}
```

## Modules

- [PubSub Ktor plugin](pubsub-ktor/README.MD) to conveniently consume messages from GCP PubSub, and publish messages to
  GCP PubSub
- [PubSub Ktor KotlinX Serialization Json](pubsub-ktor-kotlinx-serialization-json/README.MD) to conveniently consume
  messages from GCP PubSub, and publish messages to GCP PubSub using KotlinX Serialization Json
- [PubSub test](pubsub-test/README.MD) one-line testing support powered by testcontainers
- [GCP PubSub](pubsub/README.MD): KotlinX integration for `TopicAdminClient`, `SubscriptionAdminClient`, `Susbcriber`
  and `Publisher`.
- [PubSub Ktor KotlinX Serialization Json](pubsub-kotlinx-serialization-json/README.MD) to conveniently consume messages
  from GCP PubSub, and publish messages to GCP PubSub
- [Google Common API](api-core/README.MD): KotlinX integration for `ApiFuture`

## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  implementation("io.github.nomisrev:gcp-pubsub-ktor:1.0.0")
  implementation("io.github.nomisrev:gcp-pubsub-ktor-kotlinx-serialization-json:1.0.0")
  testImplementation("io.github.nomisrev:gcp-pubsub-test:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>gcp-pubsub-ktor</artifactId>
    <version>1.0.0</version>
</dependency>
```
