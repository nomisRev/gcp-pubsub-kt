package io.github.nomisrev.pubsub

import com.google.api.core.ApiFutures
import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.trySendBlocking

object GcpPubSub {

  suspend fun createTopic(
    projectId: String,
    topicId: String,
    settings: TopicAdminSettings = TopicAdminSettings.newBuilder().build(),
    context: CoroutineContext = Dispatchers.IO
  ): Unit {
    val topic = TopicName.of(projectId, topicId)
    withContext(context) {
      TopicAdminClient.create(settings).use { it.createTopic(topic) }
    }
  }

  suspend fun deleteTopic(
    projectId: String,
    topicId: String,
    settings: TopicAdminSettings = TopicAdminSettings.newBuilder().build(),
    context: CoroutineContext = Dispatchers.IO
  ) {
    val topic = TopicName.of(projectId, topicId)
    withContext(context) {
      TopicAdminClient.create(settings).use { it.deleteTopic(topic) }
    }
  }

  suspend fun createSubscription(
    projectId: String,
    subscriptionId: String,
    topicId: String,
    settings: SubscriptionAdminSettings = SubscriptionAdminSettings.newBuilder().build(),
    context: CoroutineContext = Dispatchers.IO
  ): Unit {
    val topicName = TopicName.of(projectId, topicId)
    val subscriptionName = SubscriptionName.of(projectId, subscriptionId)
    withContext(context) {
      SubscriptionAdminClient.create(settings).use { subscriptionAdminClient ->
        // create a pull subscription with default acknowledgement deadline (= 10 seconds)
        subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0)
      }
    }
  }

  suspend fun deleteSub(
    projectId: String,
    subscriptionId: String,
    settings: SubscriptionAdminSettings? = SubscriptionAdminSettings.newBuilder().build(),
    context: CoroutineContext = Dispatchers.IO
  ) {
    val sub = SubscriptionName.of(projectId, subscriptionId)
    withContext(context) {
      SubscriptionAdminClient.create(settings).use { it.deleteSubscription(sub) }
    }
  }

  /**
   * Basic implementation to subscribe to Google PubSub.
   * `A` is offloaded into a [Channel]
   * To deal with backpressure you can provide [FlowControlSettings] which allows limiting how many messages subscribers pull.
   * You can limit both the number of messages and the maximum size of messages held by the client at one time, so as to not overburden a single client.
   */
  fun subscribe(
    projectId: String,
    subscriptionId: String,
    credentialsProvider: CredentialsProvider? = null,
    channelProvider: TransportChannelProvider? = null,
    flowControlSettings: FlowControlSettings? = null
  ): Flow<PubsubRecord> =
    channelFlow {
      val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
      // Create Subscriber for projectId & subscriptionId
      val subscriber = Subscriber.newBuilder(subscriptionName) { message: PubsubMessage, consumer: AckReplyConsumer ->
        // Block the upstream when Channel cannot keep up with messages
        trySendBlocking(PubsubRecord(message, consumer))
      }.apply {
        channelProvider?.let(this::setChannelProvider)
        credentialsProvider?.let(this::setCredentialsProvider)
        flowControlSettings?.let(this::setFlowControlSettings)
      }.build()

      subscriber.startAsync()
      awaitClose { subscriber.stopAsync() }
    }

  /**
   * Publishes a stream of message [A] to a certain [projectId] and [topicId].
   * Waits until all messages are published, before it returns.
   *
   * Note: Flow<A> is sent concurrently so messages might arrive out-of-order
   */
  suspend fun <A> publish(
    messages: Flow<A>,
    projectId: String,
    topicId: String,
    encode: suspend (A) -> ByteString,
    publisher: Publisher? = null
  ): Unit =
    publish(messages.map(encode), projectId, topicId, publisher)

  /**
   * Publishes a stream of message [String] to a certain [projectId] and [topicId].
   * Waits until all messages are published, before it returns.
   * The publishing of the message may occur immediately or be delayed based on the [publisher] batching options.
   *
   * Note: Flow<ByteString> is sent concurrently so messages might arrive out-of-order
   */
  suspend fun publishUnordered(
    messages: Flow<ByteString>,
    projectId: String,
    topicId: String,
    publisher: Publisher? = null,
    context: CoroutineContext = Dispatchers.IO
  ): Unit {
    // Create publisher for projectId & topicId
    val topicName = ProjectTopicName.of(projectId, topicId)

    @Suppress("NAME_SHADOWING")
    val publisher = publisher ?: withContext(context) {
      Publisher.newBuilder(topicName).build()
    }
    // Create messages and publish them
    val futures = messages
      .map {
        PubsubMessage.newBuilder()
          .setData(it)
          .build()
      }.map(publisher::publish)
      .toList() // Collect all the futures from publishing the messages

    // Wait until all message were published & shutdown
    runInterruptible(context) {
      try {
        ApiFutures.allAsList(futures).get()
      } finally {
        publisher.shutdown()
      }
    }
  }

  suspend fun publish(
    messages: Flow<ByteString>,
    projectId: String,
    topicId: String,
    publisher: Publisher? = null,
    context: CoroutineContext = Dispatchers.IO
  ): Unit {
    // Create publisher for projectId & topicId
    val topicName = ProjectTopicName.of(projectId, topicId)

    @Suppress("NAME_SHADOWING")
    val publisher = publisher ?: withContext(context) {
      Publisher.newBuilder(topicName).build()
    }
    try {
      // Create messages and publish them
      messages.collect { bytes ->
        runInterruptible(context) {
          publisher.publish(
            PubsubMessage.newBuilder()
              .setData(bytes)
              .build()
          ).get()
        }
      }
    } finally {
      publisher.shutdown()
    }
  }
}
