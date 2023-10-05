# Module gcp-pubsub-test

Testing module build on top of testcontainers for Gcp PubSub exist out of a single type `PubSubEmulator`,
it implements `AutoCloseable`, and `Startable` for the underlying testcontainers.

In addition to `ExternalResource` such that you can easily use it with JUnit's `ClassRule` and `Rule`,
and it's also a Ktor Plugin such that you can easily install it in your `testApplication` and all configuration is done
for you.

```kotlin
class GcpPluginTest {
  val projectId = ProjectId("my-project-id")

  @Test
  fun emulatorTest() = runBlocking(Default) {
    val topic = emulator.uniqueTopic()
    val subscription = emulator.uniqueSubscription()
    val messages = (1..3).map { "$it" }
    val channel = Channel<String>(3)
    
    testApplication {
      install(emulator)
      
      application {
        pubSub(projectId) {
          createTopic(topic)
          createSubscription(subscription, topic)
          launch { publish(topic, messages) }
          
          subscribe(subscription) { record ->
            record.ack().await()
            channel.send(record.message.data.toStringUtf8())
          }
        }
      }.also { startApplication() }
      
      assertEquals(messages.toSet(), channel.consumeAsFlow().take(3).toSet())
    }
  }
  
  companion object {
    @JvmStatic
    @get:ClassRule
    val emulator = PubSubEmulator()
  }
}
```
## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  testImplementation("io.github.nomisrev:gcp-pubsub-test:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>gcp-pubsub-test</artifactId>
    <version>1.0.0</version>
    <scope>test</scope>
</dependency>
```

