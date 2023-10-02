package io.github.nomisrev.gcp.pubsub.ktor

import com.google.cloud.pubsub.v1.Subscriber
import io.github.nomisrev.gcp.pubsub.GcpPublisher
import io.github.nomisrev.gcp.pubsub.GcpPubsSubAdmin
import io.github.nomisrev.gcp.pubsub.PubsubRecord
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.DEFAULT_CONCURRENCY

public interface GcpPubSubSyntax : GcpPubsSubAdmin, GcpPublisher {
  @OptIn(FlowPreview::class)
  public fun subscribe(
    subscriptionId: SubscriptionId,
    concurrency: Int = DEFAULT_CONCURRENCY,
    context: CoroutineContext = Dispatchers.Default,
    configure: Subscriber.Builder.() -> Unit = {},
    handler: suspend (PubsubRecord) -> Unit
  ): Job
}
