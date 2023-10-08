package io.github.nomisrev.gcp.pubsub

import com.google.api.gax.rpc.AlreadyExistsException
import com.google.api.gax.rpc.InvalidArgumentException
import com.google.api.gax.rpc.NotFoundException
import io.github.nomisrev.gcp.pubsub.test.PubSubEmulator
import java.lang.AssertionError
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.AfterClass
import org.junit.ClassRule

class PubSubTest {

  @Test
  fun `Create empty topic`() = runBlocking {
    val actual = assertThrows<InvalidArgumentException> { admin.createTopic(TopicId("")) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [topics] name: (name=projects/my-project-id/topics/)"
    )
  }

  @Test
  fun `Create topic twice`() = runBlocking {
    val topicId = extension.uniqueTopic()
    admin.createTopic(topicId)
    val actual = assertThrows<AlreadyExistsException> { admin.createTopic(topicId) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: ALREADY_EXISTS: Topic already exists"
    )
  }

  @Test
  fun `Delete empty topic`() = runBlocking {
    val actual = assertThrows<InvalidArgumentException> { admin.deleteTopic(TopicId("")) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [topics] name: (name=projects/my-project-id/topics/)"
    )
  }

  @Test
  fun `Delete non-existing topic `() = runBlocking {
    val topicId = extension.uniqueTopic()
    val actual = assertThrows<NotFoundException> { admin.deleteTopic(topicId) }
    assertEquals(actual.message, "io.grpc.StatusRuntimeException: NOT_FOUND: Topic not found")
  }

  @Test
  fun `Create empty subscription`() = runBlocking {
    val topicId = extension.uniqueTopic()
    admin.createTopic(topicId)
    val actual =
      assertThrows<InvalidArgumentException> {
        admin.createSubscription(SubscriptionId(""), topicId)
      }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [subscriptions] name: (name=projects/my-project-id/subscriptions/)"
    )
  }

  @Test
  fun `Create subscription for non-existing topic`() = runBlocking {
    val topicId = extension.uniqueTopic()
    val subscriptionId = extension.uniqueSubscription()
    val actual =
      assertThrows<NotFoundException> { admin.createSubscription(subscriptionId, topicId) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: NOT_FOUND: Subscription topic does not exist"
    )
  }

  @Test
  fun `Create subscription twice`() = runBlocking {
    val topicId = extension.uniqueTopic()
    val subscriptionId = extension.uniqueSubscription()
    admin.createTopic(topicId)
    admin.createSubscription(subscriptionId, topicId)
    val actual =
      assertThrows<AlreadyExistsException> { admin.createSubscription(subscriptionId, topicId) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: ALREADY_EXISTS: Subscription already exists"
    )
  }

  @Test
  fun `Delete empty subscription`() = runBlocking {
    val actual =
      assertThrows<InvalidArgumentException> { admin.deleteSubscription(SubscriptionId("")) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [subscriptions] name: (name=projects/my-project-id/subscriptions/)"
    )
  }

  @Test
  fun `Delete non-existing subscription`() = runBlocking {
    val subscriptionId = extension.uniqueSubscription()
    val actual = assertThrows<NotFoundException> { admin.deleteSubscription(subscriptionId) }
    assertEquals(
      actual.message,
      "io.grpc.StatusRuntimeException: NOT_FOUND: Subscription does not exist"
    )
  }

  @Test
  fun `publish to non-existing topic`() = runBlocking {
    val actual = assertThrows<NotFoundException> { publisher.publish(TopicId("non-existing"), "") }
    assertEquals(actual.message, "io.grpc.StatusRuntimeException: NOT_FOUND: Topic not found")
  }

  @Suppress("MaxLineLength")
  @Test
  fun `subscribe to non-existing subscription`() = runBlocking {
    val actual =
      assertThrows<NotFoundException> {
        subscriber.subscribe(SubscriptionId("non-existing")).single()
      }
    assertEquals(
      actual.message,
      "com.google.api.gax.rpc.NotFoundException: io.grpc.StatusRuntimeException: NOT_FOUND: Subscription does not exist (resource=non-existing)"
    )
  }

  @Test
  fun `publish and subscribe multiple messages`() = runBlocking {
    val expected = listOf("first-message", "second-message", "third-message")
    val topicId = extension.uniqueTopic()
    val subscriptionId = extension.uniqueSubscription()
    admin.createTopic(topicId)
    admin.createSubscription(subscriptionId, topicId)

    publisher.publish(topicId, expected)

    val actual =
      subscriber
        .subscribe(subscriptionId)
        .map { msg ->
          val str = msg.data.toStringUtf8()
          msg.ack()
          str
        }
        .take(3)
        .toSet()

    assertEquals(expected.toSet(), actual)

    val shouldTimeout =
      withTimeoutOrNull(1.seconds) { subscriber.subscribe(subscriptionId).take(1).toList() }
    assert(shouldTimeout == null) { "Messages were not acknowledged and received twice" }
  }

  @Test
  fun `publish and subscribe ordered multiple messages`() = runBlocking {
    val expected = listOf("first-message", "second-message", "third-message")
    val topicId = extension.uniqueTopic()
    val subscriptionId = extension.uniqueSubscription()
    admin.createTopic(topicId)
    admin.createSubscription(subscriptionId, topicId)

    extension
      .publisher(projectId) { setEnableMessageOrdering(true) }
      .use { publisher -> publisher.publish(topicId, expected) { setOrderingKey("key") } }

    val actual =
      subscriber
        .subscribe(subscriptionId)
        .map { msg ->
          val str = msg.data.toStringUtf8()
          msg.ack()
          str
        }
        .take(3)
        .toList()

    assertEquals(expected, actual)

    val shouldTimeout =
      withTimeoutOrNull(1.seconds) { subscriber.subscribe(subscriptionId).take(1).toList() } == null
    assert(shouldTimeout) { "Messages were not acknowledged and received twice" }
  }

  companion object {
    val projectId = ProjectId("my-project-id")

    @JvmStatic @get:ClassRule val extension = PubSubEmulator()

    val subscriber by lazy { extension.subscriber(projectId) }
    val publisher by lazy { extension.publisher(projectId) }
    val admin by lazy { extension.admin(projectId) }

    @JvmStatic
    @AfterClass
    fun destroy() {
      admin.close()
      publisher.close()
    }
  }
}

inline fun <reified T : Throwable> assertThrows(executable: () -> Unit): T =
  when (val exception = runCatching { executable() }.exceptionOrNull()) {
    null ->
      throw AssertionError(
        "Expected ${T::class.simpleName} to be thrown, but no exception was thrown."
      )
    is T -> exception
    else ->
      throw AssertionError(
        "Expected ${T::class.simpleName} to be thrown, but found ${exception::class.simpleName}",
        exception
      )
  }
