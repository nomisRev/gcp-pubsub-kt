package io.github.nomisrev.gcp.pubsub.ktor.serialization

import com.google.cloud.pubsub.v1.Subscriber
import io.github.nomisrev.gcp.pubsub.AcknowledgeableValue
import io.github.nomisrev.gcp.pubsub.GcpSubscriber
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import io.github.nomisrev.gcp.pubsub.ktor.GcpPubSubSyntax
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.DEFAULT_CONCURRENCY
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.plus
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

/**
 * This signature looks the same as the regular [GcpPubSubSyntax.subscribe], but instead takes
 * allows processing Json values of type `A`.
 *
 * To use this method you need to explicitly pass the generic argument, otherwise use
 * [subscribeDeserialized]. Alternatively you can also use [deserialized].
 *
 * ```kotlin
 * @Serializable
 * data class Event(val key: String, val message: String)
 *
 * fun Application.pubSubApp() {
 *   pubSub(ProjectId("my-project")) {
 *     subscribe<Event>(SubscriptionId("my-subscription")) { (event, record) ->
 *       println("event.key: ${event.key}, event.message: ${event.message}")
 *       record.ack()
 *     }
 *   }
 * ```
 *
 * @see GcpSubscriber.subscribe for full documentation.
 */
@OptIn(FlowPreview::class)
public inline fun <reified A> GcpPubSubSyntax.subscribe(
  subscriptionId: SubscriptionId,
  concurrency: Int = DEFAULT_CONCURRENCY,
  context: CoroutineContext = Dispatchers.Default,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: Subscriber.Builder.() -> Unit = {},
  noinline handler: suspend (AcknowledgeableValue<A>) -> Unit,
): Job =
  subscriber
    .subscribe(subscriptionId) { configure() }
    .map { record ->
      flow {
        emit(
          handler(
            AcknowledgeableValue(
              json.decodeFromString(serializer, record.message.data.toStringUtf8()),
              record,
            )
          )
        )
      }
    }
    .flattenMerge(concurrency)
    .launchIn(this@subscribe + context)

public inline fun <reified A> GcpPubSubSyntax.subscribeDeserialized(
  subscriptionId: SubscriptionId,
  concurrency: Int = DEFAULT_CONCURRENCY,
  context: CoroutineContext = Dispatchers.Default,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: Subscriber.Builder.() -> Unit = {},
  noinline handler: suspend (AcknowledgeableValue<A>) -> Unit,
): Job = subscribe(subscriptionId, concurrency, context, json, serializer, configure, handler)
