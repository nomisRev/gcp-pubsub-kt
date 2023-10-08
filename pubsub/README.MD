# Module gcp-pubsub

KotlinX integration for `TopicAdminClient`, `SubscriptionAdminClient`, `Susbcriber` and `Publisher`.

This modules exposes following types that correspond to their relevant Gcp PubSub types but expose `suspend` APIs, that
await the operations in a suspending way.

| **Name**                          | **Description**                                                                                                                                                                         
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| [GcpPublisher](#GcpPublisher)     | Exposes `publish` methods, and automatically caches the `Publisher` under-the-hood for optimal performance. All `publish` methods suspend until all methods are published to the topic. 
| [GcpPubSubAdmin](#GcpPubSubAdmin) | Combines the API of `TopicAdminClient` and `SubscriptionAdminClient`, and awaits the succesfully result of their operations in a suspending way.                                        
| [GcpSubscriber](#GcpSubscriber)   | Exposes a single `subscribe` method that subscribes to PubSub and exposes the stream of messages as a KotlinX Flow.                                                                     
| [GcpPull]                         | Exposes a single `pull` method that pulls messages from PubSub in a back-pressured way.                                                                                                 

## Examples

All types can be created using the `ProjectId`, and a configuration lambda using the original `Builder` types from
Google's Java lib so the original documentation and techniques apply. It keeps the original default settings, see all
the examples below.

### GcpPublisher

Configuring the `GcpPublisher` can conveniently be done using the lambda using the Google `Publisher.Builder` API.
The `GcpPublisher` type implements `AutoCloseable` so most conveniently you can consume it with `use` from the Kotlin
Std. Alternatively you could also use `try/finally`, or similar strategies.

```kotlin
GcpPublisher(ProjectId("my-project")).use { publisher ->

}
```

Publishing messages from different types `String`, `ByteArray`, `ByteBuffer`, `ByteString` is support out-of-the-box,
this can be done by providing a single value or an `Iterable` of any of these types.

```kotlin
publisher.publish(TopicId("my-topic"), "msg1")
publisher.publish(TopicId("my-topic"), listOf("msg2", "msg3"))
publisher.publish(TopicId("my-topic"), "msg1".toByteArray())
```

You can also publish custom types, but need to provide a `MessageEncoder<A>`.
See [pubsub-kotlinx-serialization-json](../pubsub-kotlinx-serialization-json/README.MD) on how to publish messages
using `Json` with KotlinX Serialization.

If you want to implement your own serializers, you can use the `MessageEncoder` interface.

```kotlin
data class MyEvent(val key: String, val message: String)

object MyEventEncoder : MessageEncoder<MyEvent> {
  override suspend fun encode(value: MyEvent): PubsubMessage =
    PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8(value.message))
      .setOrderingKey(value.key)
      .build()
}

publisher.publish(TopicId("my-topic"), MyEvent("key", "msg1"), MessageEncoder)
publisher.publish(TopicId("my-topic"), listOf(MyEvent("key", "msg2"), MyEvent("key", "msg3")), MessageEncoder)
```

### GcpPubSubAdmin

Configuring the `GcpPubSubAdmin` can conveniently be done by any of the overloads, it wraps both `TopicAdminClient`
and `SubscriptionAdminClient`. You can manually construct both, and pass them into the constructor,
remember that you need to close both `TopicAdmintClient` and `SubscriptionAdminClient` so you can either use `use` or
manually close `TopicAdminClient` and `SubscriptionAdminClient`.

```kotlin
val topicAdminClient: TopicAdmintClient = TODO("Create TopicAdmintClient")
val subscriptionAdminClient: SubscriptionAdminClient = TODO("Create SubscriptionAdminClient")
GcpPubsSubAdmin(
  ProjectId("my-project"),
  topicAdminClient,
  subscriptionAdminClient
).use { admin ->

}
```

Or, there is also the option to rely on the lambdas to configure the `TopicAdminSettings.Builder`
and `SubscriptionAdminSettings.Builder`.

```kotlin
val credentials = GoogleCredentials.fromStream(FileInputStream(PATH_TO_JSON_KEY))

GcpPubsSubAdmin(
  ProjectId("my-project"),
  configureSubscriptionAdmin = { setCredentialsProvider(credentials) },
  configureTopicAdmin = { setCredentialsProvider(credentials) }
).use { admin ->

}
```

Once the admin is created we can use all of its convenient `suspend` functions.

```kotlin
GcpPubsSubAdmin(ProjectId("my-project")).use { admin ->
  admin.createTopic(TopicId("my-topic"))
  admin.createSubscription(Subsriptionid("my-subscription"), TopicId("my-topic"))
  // delete, get or list topics and subscriptions
}
```

### GcpSubscriber

The `GcpSubscriber` can easily be created by just providing a `ProjectId`, since a new [Subscriber] will be created for
every [subscribe] call that is made. Optionally, a common `configure` lambda can be provided for configuration that is
common for all [Subscriber] instances. `GcpSubscriber` doesn't implement `AutoCloseable` as it internally takes care of
the lifecycles of the created [Subscriber].

```kotlin
val subscriber = GcpSubscriber(ProjectId("my-project"))

val subscriber2 = GcpSubscriber(ProjectId("my-project")) {
  setCredentialsProvider(GoogleCredentials.fromStream(FileInputStream(PATH_TO_JSON_KEY)))
  setParallelPullCount(3)
  setMaxDurationPerAckExtension(Duration.ofMinutes(60))
}
```

Then we can simply call the `subscribe` method, of which two simple variants exist to subscribe to a
given [SubscriptionId].

```kotlin
val subscription = SubsriptionId("my-subscription")
val subscriber: GcpSubscriber = TODO("Create subscriber")
subscriber.subscribe(subscription)
  .collect { record ->
    println("Processing - ${record.message.data.toStringUtf8()}")
    record.ack()
  }
```

There is also a variant that takes a `MessageDecoder` in case you want to process decoded messages of ` A`.

```kotlin
data class Event(val key: String, val message: String)

val eventDecoder = object : MessageDecoder<Event> {
  override suspend fun decode(message: PubsubMessage): A =
    Event(message.key, message.data.toStringUtf8())
}

val subscription = SubsriptionId("my-subscription")
val subscriber: GcpSubscriber = TODO("Create subscriber")
subscriber.subscribe(subscription, eventDecoder)
  .collect { (event: Event, record: PubsubRecord) ->
    println("event.key: ${event.key}, event.message: ${event.message}")
    record.ack()
  }
```

## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  implementation("io.github.nomisrev:gcp-pubsub:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>gcp-pubsub</artifactId>
    <version>1.0.0</version>
</dependency>
```
