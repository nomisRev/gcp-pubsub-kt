package io.github.nomisrev.gcp.pubsub

import com.google.api.core.ApiService
import com.google.api.gax.batching.FlowControlSettings
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse
import com.google.cloud.pubsub.v1.Subscriber
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.PubsubMessage
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow

public fun GcpSubscriber(
  projectId: ProjectId? = null,
  configure: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit = {}
): GcpSubscriber = DefaultGcpSubscriber(projectId, configure)

public interface GcpSubscriber {

  /**
   * Basic implementation to subscribe to Google PubSub. `A` is offloaded into a [Channel] To deal
   * with backpressure you can provide [FlowControlSettings], which allows limiting how many
   * messages subscribers pull. You can limit both the number of messages and the maximum size of
   * messages held by the client at one time, to not overburden a single client.
   */
  public fun subscribe(
    subscriptionId: SubscriptionId,
    configure: Subscriber.Builder.() -> Unit = {},
  ): Flow<PubsubRecord>
}

/** Default implementation build on top of Gcloud library */
private class DefaultGcpSubscriber(
  val projectId: ProjectId?,
  val globalConfigure: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit = {}
) : GcpSubscriber {

  override fun subscribe(
    subscriptionId: SubscriptionId,
    configure: Subscriber.Builder.() -> Unit
  ): Flow<PubsubRecord> = channelFlow {
    val subscriptionName = projectSubscriptionName(subscriptionId.value, projectId?.value)

    // Create Subscriber for projectId & subscriptionId
    val subscriber =
      Subscriber.newBuilder(subscriptionName) {
          message: PubsubMessage,
          consumer: AckReplyConsumerWithResponse ->
          // Block the upstream when Channel cannot keep up with messages
          trySendBlocking(
              PubsubRecord(message, consumer, ProjectId(subscriptionName.project), subscriptionId)
            )
            .getOrThrow()
        }
        .apply {
          globalConfigure(subscriptionId)
          configure()
        }
        .build()

    subscriber
      .apply {
        addListener(
          object : ApiService.Listener() {
            override fun failed(from: ApiService.State?, failure: Throwable?) {
              super.failed(from, failure)
              close(failure)
            }
          },
          MoreExecutors.directExecutor()
        )
      }
      .startAsync()

    awaitClose { subscriber.stopAsync().awaitTerminated() }
  }
}
