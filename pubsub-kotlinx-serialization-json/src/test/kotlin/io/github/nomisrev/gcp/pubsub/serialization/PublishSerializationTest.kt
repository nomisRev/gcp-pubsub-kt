package io.github.nomisrev.gcp.pubsub.serialization

import io.github.nomisrev.gcp.pubsub.ProjectId
import io.github.nomisrev.gcp.pubsub.test.PubSubEmulator
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.AfterClass
import org.junit.ClassRule

class PublishSerializationTest {
  @Serializable data class Event(val id: Long, val content: String)

  @Test
  fun canSerializeAndDeserialize() =
    runBlocking(Default) {
      val topic = pubSubEmulator.uniqueTopic()
      val subscription = pubSubEmulator.uniqueSubscription()
      admin.createTopic(topic)
      admin.createSubscription(subscription, topic)

      val expected = listOf(Event(1, "msg1"), Event(1, "msg2"))
      val event = Event(1, "msg3")
      publisher.publish(topic, expected)
      publisher.publish(topic, event)

      val actual =
        pubSubEmulator
          .subscriber(projectId)
          .subscribe<Event>(subscription)
          .map { (event, record) -> event.also { record.ack() } }
          .take(3)
          .toList()

      assertEquals((expected + event).toSet(), actual.toSet())
    }

  companion object {
    @get:ClassRule @JvmStatic val pubSubEmulator = PubSubEmulator()

    val projectId = ProjectId("my-project-id")
    val admin by lazy { pubSubEmulator.admin(projectId) }
    val publisher by lazy { pubSubEmulator.publisher(projectId) }

    @AfterClass
    @JvmStatic
    fun destroy() {
      admin.close()
      publisher.close()
    }
  }
}
