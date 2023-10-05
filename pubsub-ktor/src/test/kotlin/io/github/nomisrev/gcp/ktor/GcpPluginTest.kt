package io.github.nomisrev.gcp.ktor

import com.google.cloud.pubsub.v1.AckResponse
import io.github.nomisrev.gcp.core.await
import io.github.nomisrev.gcp.pubsub.ProjectId
import io.github.nomisrev.gcp.pubsub.ktor.pubSub
import io.github.nomisrev.gcp.pubsub.publish
import io.github.nomisrev.gcp.pubsub.test.PubSubEmulator
import io.ktor.server.testing.testApplication
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test

class GcpPluginTest {
  val projectId = ProjectId("my-project-id")

  @Test
  fun canReceiveAndProcess3Elements() =
    runBlocking(Default) {
      val topic = pubSubEmulator.uniqueTopic()
      val subscription = pubSubEmulator.uniqueSubscription()

      testApplication {
        install(pubSubEmulator)

        val messages = (1..3).map { "$it" }
        val latch = Channel<String>(3)

        pubSubEmulator.admin(projectId).createTopic(topic)
        pubSubEmulator.admin(projectId).createSubscription(subscription, topic)

        application {
          pubSub(projectId) {
            subscribe(subscription) { record ->
              assertEquals(record.ack().await(), AckResponse.SUCCESSFUL)
              latch.send(record.message.data.toStringUtf8())
            }
          }
        }

        startApplication()
        launch(IO) { pubSubEmulator.publisher(projectId).publish(topic, messages) }

        assertEquals(messages.toSet(), latch.consumeAsFlow().take(3).toSet())
      }
    }

  companion object {
    @JvmStatic @get:ClassRule val pubSubEmulator = PubSubEmulator()
  }
}
