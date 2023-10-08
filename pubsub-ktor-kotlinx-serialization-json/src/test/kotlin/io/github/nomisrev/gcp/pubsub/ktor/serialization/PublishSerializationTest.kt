package io.github.nomisrev.gcp.pubsub.ktor.serialization

import com.google.cloud.pubsub.v1.AckResponse
import io.github.nomisrev.gcp.core.await
import io.github.nomisrev.gcp.pubsub.ProjectId
import io.github.nomisrev.gcp.pubsub.ktor.pubSub
import io.github.nomisrev.gcp.pubsub.serialization.publish
import io.github.nomisrev.gcp.pubsub.test.PubSubEmulator
import io.ktor.server.testing.testApplication
import junit.framework.TestCase
import kotlin.test.assertEquals
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.ClassRule

class PublishSerializationTest {
  @Serializable data class Event(val id: Long, val content: String)

  val projectId = ProjectId("my-project-id")

  @org.junit.Test
  fun canReceiveAndProcess3Elements() =
    runBlocking(Default) {
      val topic = pubSubEmulator.uniqueTopic()
      val subscription = pubSubEmulator.uniqueSubscription()

      val messages = (1..3L).map { Event(1, "msg$it") }

      testApplication {
        install(pubSubEmulator)

        val latch = Channel<Event>(3)

        pubSubEmulator.admin(projectId).createTopic(topic)
        pubSubEmulator.admin(projectId).createSubscription(subscription, topic)

        application {
          pubSub(projectId) {
            subscribe<Event>(subscription) { (event, record) ->
              assertEquals(record.ack().await(), AckResponse.SUCCESSFUL)
              latch.send(event)
            }
          }
        }

        startApplication()
        launch(Dispatchers.IO) { pubSubEmulator.publisher(projectId).publish(topic, messages) }

        TestCase.assertEquals(messages.toSet(), latch.consumeAsFlow().take(3).toSet())
      }
    }

  companion object {
    @JvmStatic @get:ClassRule val pubSubEmulator: PubSubEmulator = PubSubEmulator()
  }
}
