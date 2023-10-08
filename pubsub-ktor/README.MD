# Module gcp-pubsub-ktor

Ktor plugin offering convenient way of working with GCP PubSub, it offers a simplified way of configuring Gcp PubSub and
automatically takes care of lifecycle to match lifecycle of the Ktor Application.

Simply `install` the plugin into Ktor Application, and set up the desired configuration.

```kotlin
fun Application.setup(): GcpPubSub =
  install(GcpPubSub) {
    // all configuration is optional
    credentialsProvider = GoogleCredentials.fromStream(FileInputStream(PATH_TO_JSON_KEY))
    subscriber { /* configure subscriber */ }
    publisher { /* configure publisher */ }
    topicAdmin { /* configure topic admin */ }
    subscriptionAdmin { /* configure subscription admin */ }
  }
```

After you've installed the `GcpPubSub` plugin, and setup according the needs of your application simply open a `pubSub`
block inside the Ktor Application and you'll have access to all GcpPubSub functionality through the DSL.

```kotlin
fun Application.setup(topic: TopicId, subscription: SubscriptionId): Job =
  pubSub(ProjectId("my-project")) {
    publish(topic, messages)

    subscribe(subscription) { record ->
      println(record.message.data.toStringUtf8())
      record.ack()
    }
  }
```

You can also easily access the `GcpPubSub` plugin using the `pubSub` function in case you need access to `publisher`
from a route, or different parts from your application.

```kotlin
fun Application.route(): Routing =
  routing {
    post("/publish/{message}") {
      val message =
        requireNotNull(call.parameters["message"]) { "Missing parameter message" }
      pubSub().publisher(ProjectId("my-project"))
        .publish(TopicId("my-topic"), message)
      call.respond(HttpStatusCode.Accepted)
    }
  }
```
