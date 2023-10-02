package io.github.nomisrev.gcp.pubsub

import com.google.api.gax.rpc.AlreadyExistsException
import com.google.api.gax.rpc.InvalidArgumentException
import com.google.api.gax.rpc.NotFoundException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.withTimeoutOrNull

class PubSubTest :
  StringSpec({
    val projectId = ProjectId("my-project-id")
    val extension = install(PubSubEmulatorExtension())
    val subscriber = extension.subscriber(projectId)
    val publisher = autoClose(extension.publisher(projectId))
    val admin = autoClose(extension.admin(projectId))

    "Create empty topic" {
      shouldThrow<InvalidArgumentException> { admin.createTopic(TopicId("")) }.message shouldBe
        "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [topics] name: (name=projects/my-project-id/topics/)"
    }

    "Create topic twice" {
      val topicId = extension.uniqueTopic()
      admin.createTopic(topicId)
      shouldThrow<AlreadyExistsException> { admin.createTopic(topicId) }.message shouldBe
        "io.grpc.StatusRuntimeException: ALREADY_EXISTS: Topic already exists"
    }

    "Delete empty topic " {
      shouldThrow<InvalidArgumentException> { admin.deleteTopic(TopicId("")) }.message shouldBe
        "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [topics] name: (name=projects/my-project-id/topics/)"
    }

    "Delete non-existing topic " {
      val topicId = extension.uniqueTopic()
      shouldThrow<NotFoundException> { admin.deleteTopic(topicId) }.message shouldBe
        "io.grpc.StatusRuntimeException: NOT_FOUND: Topic not found"
    }

    "Create empty subscription" {
      val topicId = extension.uniqueTopic()
      admin.createTopic(topicId)
      shouldThrow<InvalidArgumentException> {
          admin.createSubscription(SubscriptionId(""), topicId)
        }
        .message shouldBe
        "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [subscriptions] name: (name=projects/my-project-id/subscriptions/)"
    }

    "Create subscription for non-existing topic" {
      val topicId = extension.uniqueTopic()
      val subscriptionId = extension.uniqueSubscription()
      shouldThrow<NotFoundException> { admin.createSubscription(subscriptionId, topicId) }
        .message shouldBe
        "io.grpc.StatusRuntimeException: NOT_FOUND: Subscription topic does not exist"
    }

    "Create subscription twice" {
      val topicId = extension.uniqueTopic()
      val subscriptionId = extension.uniqueSubscription()
      admin.createTopic(topicId)
      admin.createSubscription(subscriptionId, topicId)
      shouldThrow<AlreadyExistsException> { admin.createSubscription(subscriptionId, topicId) }
        .message shouldBe
        "io.grpc.StatusRuntimeException: ALREADY_EXISTS: Subscription already exists"
    }

    "Delete empty subscription " {
      shouldThrow<InvalidArgumentException> { admin.deleteSubscription(SubscriptionId("")) }
        .message shouldBe
        "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid [subscriptions] name: (name=projects/my-project-id/subscriptions/)"
    }

    "Delete non-existing subscription " {
      val subscriptionId = extension.uniqueSubscription()
      shouldThrow<NotFoundException> { admin.deleteSubscription(subscriptionId) }.message shouldBe
        "io.grpc.StatusRuntimeException: NOT_FOUND: Subscription does not exist"
    }

    "publish to non-existing topic" {
      val actual = shouldThrow<NotFoundException> { publisher.publish(TopicId("non-existing"), "") }
      actual.message shouldBe "io.grpc.StatusRuntimeException: NOT_FOUND: Topic not found"
    }

    @Suppress("MaxLineLength")
    "subscribe to non-existing subscription" {
      val actual =
        shouldThrow<NotFoundException> {
          subscriber.subscribe(SubscriptionId("non-existing")).single()
        }
      actual.message shouldBe
        "com.google.api.gax.rpc.NotFoundException: io.grpc.StatusRuntimeException: NOT_FOUND: Subscription does not exist (resource=non-existing)"
    }

    "publish and subscribe multiple messages" {
      val messages = listOf("first-message", "second-message", "third-message")
      val topicId = extension.uniqueTopic()
      val subscriptionId = extension.uniqueSubscription()
      admin.createTopic(topicId)
      admin.createSubscription(subscriptionId, topicId)

      publisher.publish(topicId, messages)

      subscriber
        .subscribe(subscriptionId)
        .map { msg ->
          val str = msg.data.toStringUtf8()
          msg.ack()
          str
        }
        .take(3)
        .toSet() shouldBe messages.toSet()

      val shouldTimeout =
        withTimeoutOrNull(1.seconds) { subscriber.subscribe(subscriptionId).take(1).toList() } ==
          null
      assert(shouldTimeout) { "Messages were not acknowledged and received twice" }
    }

    "publish and subscribe ordered multiple messages" {
      val messages = listOf("first-message", "second-message", "third-message")
      val topicId = extension.uniqueTopic()
      val subscriptionId = extension.uniqueSubscription()
      admin.createTopic(topicId)
      admin.createSubscription(subscriptionId, topicId)

      extension
        .publisher(projectId) { setEnableMessageOrdering(true) }
        .use { publisher -> publisher.publish(topicId, messages) { setOrderingKey("key") } }

      subscriber
        .subscribe(subscriptionId)
        .map { msg ->
          val str = msg.data.toStringUtf8()
          msg.ack()
          str
        }
        .take(3)
        .toList() shouldBe messages

      val shouldTimeout =
        withTimeoutOrNull(1.seconds) { subscriber.subscribe(subscriptionId).take(1).toList() } ==
          null
      assert(shouldTimeout) { "Messages were not acknowledged and received twice" }
    }
  })
