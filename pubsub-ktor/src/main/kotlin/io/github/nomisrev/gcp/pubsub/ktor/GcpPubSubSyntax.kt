package io.github.nomisrev.gcp.pubsub.ktor

import com.google.cloud.pubsub.v1.Subscriber
import io.github.nomisrev.gcp.pubsub.GcpPublisher
import io.github.nomisrev.gcp.pubsub.GcpPubsSubAdmin
import io.github.nomisrev.gcp.pubsub.GcpSubscriber
import io.github.nomisrev.gcp.pubsub.PubsubRecord
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.DEFAULT_CONCURRENCY

public interface GcpPubSubSyntax : GcpPubsSubAdmin, GcpPublisher, CoroutineScope {

  public val subscriber: GcpSubscriber

  /**
   * Subscribe to [subscriptionId], and process the received [PubsubRecord] by running [handler] in
   * [concurrency] parallel coroutines
   *
   * ```kotlin
   * fun Application.process(): Job =
   *   pubSub(ProjectId("my-project")) {
   *     subscribe(SubscriptionId("my-subscription")) { record ->
   *       println(record.message.data.toStringUtf8())
   *       record.ack().
   *     }
   *   }
   * ```
   *
   * @param subscriptionId the subscription to subscribe to
   * @param concurrency the amount of parallel coroutines that will be processing [PubsubRecord]
   * @param context the [CoroutineDispatcher] where [handler] will be running
   * @param configure additional configuration to use when creating the [Subscriber], in addition to
   *   the global [GcpPubSub.Configuration.subscriber] configuration.
   * @param handler to use when processing received [PubsubRecord] messages
   */
  @OptIn(FlowPreview::class)
  public fun subscribe(
    subscriptionId: SubscriptionId,
    concurrency: Int = DEFAULT_CONCURRENCY,
    context: CoroutineContext = Dispatchers.Default,
    configure: Subscriber.Builder.() -> Unit = {},
    handler: suspend (PubsubRecord) -> Unit,
  ): Job
}
