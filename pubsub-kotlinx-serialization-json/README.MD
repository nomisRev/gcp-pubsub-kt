# Module gcp-pubsub-kotlinx-serialization-json

An integration module that offers simplified APIs that use KotlinX serialization to encode/decode messages to-and-from
Json values.

This modules exposes following types that correspond to their relevant Gcp PubSub types but expose `suspend` APIs, that
await the operations in a suspending way.

| **Name**                          | **Description**                                                                                                        
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------
| [KotlinXJsonEncoder]              | An implementation of `MessageEncoder` using `StringFormat` based serializers from KotlinX Serialization                
| [KotlinXJsonDecoder]              | An implementation of `MessageDecoder` using `StringFormat` based serializers from KotlinX Serialization                
| [publish]                         | Extension on `GcpPublisher` that allows publish types annotation with `@Serializable` using KotlinX Serialization Json 
| [deserialized]                    | Allows deserializing [PubsubMessage.getData] from [PubsubRecord] using KotlinX serialization.                          
| [subscribe/subscribeDeserialized] | Subscribe to a subscription to process types annotation with `@Serializable` using KotlinX Serialization Json          

## Example

```kotlin
@Serializable
data class Event(val key: String, val message: String)

val topic = TopicId("my-topic")
val subscription = SubsriptionId("my-subscription")
val publisher: GcpPublisher = TODO("Create publisher")
val subscriber: GcpSubscriber = TODO("Create subscriber")

publisher.publish(topic, Event("key", "my-message-1"))
publisher.publish(topic, listOf(Event("key", "my-message-2"), Event("key", "my-message-3")))

subscriber.subscribe<Event>(subscription)
  .collect { (event: Event, record: PubsubRecord) ->
    println("event.key: ${event.key}, event.message: ${event.message}")
    assertEquals(event, record.deserialized<Event>())
    record.ack()
  }
```

## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  implementation("io.github.nomisrev:gcp-pubsub-kotlinx-serialization-json:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>gcp-pubsub-kotlinx-serialization-json</artifactId>
    <version>1.0.0</version>
</dependency>
```
