package io.github.nomisrev.gcp.pubsub

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.protobuf.Empty
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PubsubMessageOrBuilder
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.ReceivedMessage
import io.github.nomisrev.gcp.core.asDeferred
import io.github.nomisrev.gcp.core.await
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow

public fun GcpPull(projectId: ProjectId): GcpPull = DefaultGcpPull(projectId)

public interface GcpPull {
  /**
   * Create an infinite stream [Flow] of [AckPubSubMessage] objects.
   *
   * The [Flow] respects backpressure by using of Pub/Sub Synchronous Pull to retrieve batches of up
   * to the requested number of messages until the full demand is fulfilled or subscription
   * terminated.
   *
   * Any exceptions that are thrown by the Pub/Sub client will be passed as an error to the stream.
   * The error handling operators, like [Flow.retry], can be used to recover and continue streaming
   * messages.
   *
   * Uses [Channel.RENDEZVOUS] such that Gcp PubSub is pulled whilst the downstream is processing,
   * use the [buffer] operator if you want to increase the amount of [SubscriberStub.pullCallable]
   * you want to be in memory. This can positively influence the latency, and throughput but
   * increase memory usage.
   *
   * @param subscription subscription from which to retrieve messages.
   * @param maxMessages max number of messages that may be pulled from the source subscription in
   * @return infinite [Flow] of [AckPubSubMessage] objects.
   */
  public fun pull(
    subscription: SubscriptionId,
    maxMessages: Int = Int.MAX_VALUE,
    configure: SubscriberStubSettings.Builder.() -> Unit = {},
  ): Flow<List<AckPubSubMessage>>
}

private class DefaultGcpPull(val projectId: ProjectId) : GcpPull {
  override fun pull(
    subscription: SubscriptionId,
    maxMessages: Int,
    configure: SubscriberStubSettings.Builder.() -> Unit,
  ): Flow<List<AckPubSubMessage>> {
    val pullRequest: PullRequest =
      PullRequest.newBuilder()
        .setSubscription(ProjectSubscriptionName.of(projectId.value, subscription.value).toString())
        .setMaxMessages(maxMessages)
        .build()

    val subscriberStub = SubscriberStubSettings.newBuilder().apply(configure).build().createStub()

    return flow<List<AckPubSubMessage>> {
        while (true) {
          subscriberStub.pullOnce(pullRequest)
        }
      }
      .buffer(Channel.RENDEZVOUS)
  }

  private suspend fun SubscriberStub.pullOnce(pullRequest: PullRequest): List<AckPubSubMessage> =
    pullCallable()
      .futureCall(pullRequest)
      .await()
      .receivedMessagesList
      .toAckPubSubMessage(this, SubscriptionId(pullRequest.subscription))

  private fun List<ReceivedMessage>.toAckPubSubMessage(
    subscriberStub: SubscriberStub,
    subscription: SubscriptionId,
  ): List<AckPubSubMessage> = map { message ->
    DefaultAckPubSubMessage(projectId, subscription, message.message, message.ackId, subscriberStub)
  }

  private fun SubscriberStub.ack(
    subscriptionId: String,
    ackIds: Collection<String>,
  ): Deferred<Empty> {
    val acknowledgeRequest =
      AcknowledgeRequest.newBuilder().addAllAckIds(ackIds).setSubscription(subscriptionId).build()
    return acknowledgeCallable().futureCall(acknowledgeRequest).asDeferred()
  }

  private fun SubscriberStub.modifyAckDeadline(
    subscriptionId: String,
    ackIds: Collection<String>,
    ackDeadlineSeconds: Int,
  ): Deferred<Empty> {
    val modifyAckDeadlineRequest =
      ModifyAckDeadlineRequest.newBuilder()
        .setAckDeadlineSeconds(ackDeadlineSeconds)
        .addAllAckIds(ackIds)
        .setSubscription(subscriptionId)
        .build()
    return modifyAckDeadlineCallable().futureCall(modifyAckDeadlineRequest).asDeferred()
  }

  private inner class DefaultAckPubSubMessage(
    override val projectId: ProjectId,
    override val subscriptionId: SubscriptionId,
    override val pubSubMessage: PubsubMessage,
    override val ackId: String,
    private val subscriberStub: SubscriberStub,
  ) : AckPubSubMessage, PubsubMessageOrBuilder by pubSubMessage {
    override fun ack(): Deferred<Empty> = subscriberStub.ack(subscriptionId.value, listOf(ackId))

    override fun modifyAckDeadline(ackDeadlineSeconds: Int): Deferred<Empty> =
      subscriberStub.modifyAckDeadline(subscriptionId.value, listOf(ackId), ackDeadlineSeconds)

    override fun toString(): String =
      """
      AckPubSubMessage {
        projectId = $projectId,
        subscriptionId = $subscriptionId,
        message = $pubSubMessage,
        ackId = $ackId
      }
    """
        .trimIndent()
  }
}
