package io.github.nomisrev.gcp.ktor

import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.github.nomisrev.gcp.pubsub.ProjectId
import io.github.nomisrev.gcp.pubsub.ktor.pubSub
import io.github.nomisrev.pubsub.PubSubEmulator
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toSet

class GcpPluginTest :
  StringSpec({
    val projectId = ProjectId("my-project-id")
    val pubSubEmulator = install(PubSubEmulator())

    "Can receive and process 3 messages" {
      val topic = pubSubEmulator.uniqueTopic()
      val subscription = pubSubEmulator.uniqueSubscription()

      testApplication {
        install(pubSubEmulator)

        val messages =
          (1..3).map { PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("$it")).build() }

        val latch = Channel<PubsubMessage>(3)

        application {
          pubSub(projectId) {
            createTopic(topic)
            createSubscription(subscription, topic)

            publish(topic, messages)

            subscribe(subscription) { record ->
              record.ack()
              latch.send(record.message)
            }
          }
        }

        startApplication()

        latch.consumeAsFlow().take(3).map { it.data }.toSet() shouldBe
          messages.map { it.data }.toSet()
      }
    }
  })
