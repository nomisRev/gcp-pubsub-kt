# Module gcp-pubsub-ktor-kotlinx-serialization-json

An integration module that offers simplified APIs that use KotlinX serialization to subscribe to Json values.
It's an addition to [Gcp PubSub KotlinX Serialization Json](../pubsub-kotlinx-serialization-json/README.MD) for Ktor.

| **Name**                          | **Description**                                                                                               
|-----------------------------------|---------------------------------------------------------------------------------------------------------------                       
| [subscribe/subscribeDeserialized] | Subscribe to a subscription to process types annotation with `@Serializable` using KotlinX Serialization Json 

## Example

```kotlin
@Serializable
data class Event(val key: String, val message: String)

fun Application.pubSub(): Job =
  pubSub(ProjectId("my-project")) {
    subscribe<Event>(SubscriptionId("my-subscription")) { (event, record) ->
      println("event.key: ${event.key}, event.message: ${event.message}")
      record.ack()
    }
  }
```

## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  implementation("io.github.nomisrev:gcp-pubsub-ktor-kotlinx-serialization-json:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>gcp-pubsub-ktor-kotlinx-serialization-json</artifactId>
    <version>1.0.0</version>
</dependency>
```
