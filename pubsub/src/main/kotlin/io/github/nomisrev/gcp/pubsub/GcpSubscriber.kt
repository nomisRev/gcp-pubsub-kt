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
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map

public interface MessageDecoder<A> {
  public suspend fun decode(message: PubsubMessage): A
}

public fun GcpSubscriber(
  projectId: ProjectId? = null,
  configure: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit = {},
): GcpSubscriber = DefaultGcpSubscriber(projectId, configure)

public interface GcpSubscriber {

  /**
   * Basic implementation to subscribe to Google PubSub.
   *
   * `PubsubRecord` is offloaded into a [Channel] with [Channel.RENDEZVOUS], and you can use
   * [Flow.buffer] to increase the size of the [Channel], if you want to keep more message in memory
   * before back-pressuring the [Subscriber].
   *
   * ```kotlin
   * val subscription = SubsriptionId("my-subscription")
   * val subscriber: GcpSubscriber = TODO("Create subscriber")
   * subscriber.subscribe(subscription)
   *   .collect { record ->
   *     println("Processing - ${record.message.data.toStringUtf8()}")
   *     record.ack()
   *   }
   * ```
   *
   * @param subscriptionId the subscription you want to subscribe to
   * @param configure the [Subscriber] using [Subscriber.Builder]. To deal with backpressure you can
   *   provide [FlowControlSettings], which allows limiting how many messages subscribers pull. You
   *   can limit both the number of messages and the maximum size of messages held by the client at
   *   one time, to not overburden a single client. See [Subscriber] for more details.
   * @return [Flow] with the received [PubsubRecord].
   */
  public fun subscribe(
    subscriptionId: SubscriptionId,
    configure: Subscriber.Builder.() -> Unit = {},
  ): Flow<PubsubRecord>

  /**
   * Utility variant that automatically decodes [PubsubRecord] into [A] using [MessageDecoder].
   *
   * ```kotlin
   * data class Event(val key: String, val message: String)
   *
   * val eventDecoder = object : MessageDecoder<Event> {
   *   override suspend fun decode(message: PubsubMessage): A =
   *     Event(message.key, message.data.toStringUtf8())
   * }
   *
   * val subscription = SubsriptionId("my-subscription")
   * val subscriber: GcpSubscriber = TODO("Create subscriber")
   * subscriber.subscribe(subscription, eventDecoder)
   *   .collect { (event: Event, record: PubsubRecord) ->
   *     println("event.key: ${event.key}, event.message: ${event.message}")
   *     record.ack()
   *   }
   * ```
   *
   * @see subscribe for full documentation
   */
  public fun <A> subscribe(
    subscriptionId: SubscriptionId,
    decoder: MessageDecoder<A>,
    configure: Subscriber.Builder.() -> Unit = {},
  ): Flow<AcknowledgeableValue<A>> =
    subscribe(subscriptionId, configure).map { record ->
      AcknowledgeableValue(decoder.decode(record.message), record)
    }
}

/** Default implementation build on top of Gcloud library */
private class DefaultGcpSubscriber(
  val projectId: ProjectId?,
  val globalConfigure: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit = {},
) : GcpSubscriber {

  override fun subscribe(
    subscriptionId: SubscriptionId,
    configure: Subscriber.Builder.() -> Unit,
  ): Flow<PubsubRecord> =
    channelFlow {
        val projectSubscriptionName = subscriptionId.toProjectSubscriptionName(projectId)

        // Create Subscriber for projectId & subscriptionId
        val subscriber =
          Subscriber.newBuilder(projectSubscriptionName) {
              message: PubsubMessage,
              consumer: AckReplyConsumerWithResponse ->
              // Block the upstream when Channel cannot keep up with messages
              trySendBlocking(
                  PubsubRecord(
                    message,
                    consumer,
                    ProjectId(projectSubscriptionName.project),
                    subscriptionId,
                  )
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
              MoreExecutors.directExecutor(),
            )
          }
          .startAsync()

        awaitClose { subscriber.stopAsync().awaitTerminated() }
      }
      .buffer(Channel.RENDEZVOUS)
}
