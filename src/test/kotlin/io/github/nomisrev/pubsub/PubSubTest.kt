package io.github.nomisrev.pubsub

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import io.grpc.ManagedChannelBuilder
import io.kotest.core.spec.style.StringSpec
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.protobuf.ByteString
import com.google.pubsub.v1.TopicName
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toSet
import arrow.fx.coroutines.Resource

class PubSubTest : StringSpec({

  val container = PubSubEmulatorContainer(
    DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators")
  )

  beforeTest { container.start() }
  afterTest { container.stop() }

  val projectId = "my-project-id"
  val topicId = "my-topic-id"
  val subscriptionId = "my-subscription-id"

  fun managedChannel(): Resource<TransportChannelProvider> =
    Resource(
      { ManagedChannelBuilder.forTarget(container.emulatorEndpoint).usePlaintext().build() },
      { channel, _ -> channel.shutdown() }
    ).map { channel -> FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)) }

  fun topicSettings(
    channel: TransportChannelProvider,
    credentialsProvider: CredentialsProvider
  ): TopicAdminSettings = TopicAdminSettings.newBuilder()
    .setTransportChannelProvider(channel)
    .setCredentialsProvider(credentialsProvider)
    .build()

  fun subSettings(
    channel: TransportChannelProvider,
    credentials: CredentialsProvider
  ): SubscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
    .setTransportChannelProvider(channel)
    .setCredentialsProvider(credentials)
    .build()

  fun publisher(
    projectId: String,
    topicId: String,
    channel: TransportChannelProvider,
    credentials: CredentialsProvider
  ): Publisher = Publisher.newBuilder(TopicName.of(projectId, topicId))
    .setChannelProvider(channel)
    .setCredentialsProvider(credentials)
    .build()

  "publish multiple messages" {
    val messages = setOf("first-message", "second-message", "third-message")
    managedChannel().use { channel ->
      val credentials = NoCredentialsProvider.create()

      GcpPubSub.createTopic(projectId, topicId, topicSettings(channel, credentials))

      GcpPubSub.createSubscription(projectId, subscriptionId, topicId, subSettings(channel, credentials))

      GcpPubSub.publish(
        messages.map(ByteString::copyFromUtf8).asFlow(),
        projectId,
        topicId,
        publisher(projectId, topicId, channel, credentials)
      )

      GcpPubSub.subscribe(
        projectId,
        subscriptionId,
        credentials,
        channel
      ) { msg ->
        msg.data.toStringUtf8()
      }.take(3).toSet() shouldBe messages
    }
  }

  "publishOrdered multiple messages" {
    val messages = listOf("first-message", "second-message", "third-message")
    managedChannel().use { channel ->
      val credentials = NoCredentialsProvider.create()

      GcpPubSub.createTopic(projectId, topicId, topicSettings(channel, credentials))

      GcpPubSub.createSubscription(projectId, subscriptionId, topicId, subSettings(channel, credentials))

      GcpPubSub.publish(
        messages.map(ByteString::copyFromUtf8).asFlow(),
        projectId,
        topicId,
        publisher(projectId, topicId, channel, credentials)
      )

      GcpPubSub.subscribe(
        projectId,
        subscriptionId,
        credentials,
        channel
      ) { msg ->
        msg.data.toStringUtf8()
      } // Receiving messages in order is not guaranteed unless you send message to the same region
        // With setEnableMessageOrdering enabled
        // https://cloud.google.com/pubsub/docs/publisher#using_ordering_keys
        .take(3).toSet() shouldBe messages.toSet()
    }
  }
})
