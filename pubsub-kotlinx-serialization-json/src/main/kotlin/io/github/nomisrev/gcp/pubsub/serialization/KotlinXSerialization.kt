package io.github.nomisrev.gcp.pubsub.serialization

import com.google.cloud.pubsub.v1.Subscriber
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.github.nomisrev.gcp.pubsub.AcknowledgeableValue
import io.github.nomisrev.gcp.pubsub.GcpPublisher
import io.github.nomisrev.gcp.pubsub.GcpSubscriber
import io.github.nomisrev.gcp.pubsub.MessageDecoder
import io.github.nomisrev.gcp.pubsub.MessageEncoder
import io.github.nomisrev.gcp.pubsub.PubsubRecord
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import io.github.nomisrev.gcp.pubsub.TopicId
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.KSerializer
import kotlinx.serialization.StringFormat
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

/**
 * KotlinX Serialization implementation of [MessageEncoder]. It's fixed to [StringFormat], so can be
 * re-used for other `string` based serializations.
 */
public class KotlinXJsonEncoder<A>(
  private val stringFormat: StringFormat,
  private val serializer: KSerializer<A>,
  private val configure: PubsubMessage.Builder.() -> Unit
) : MessageEncoder<A> {
  override suspend fun encode(value: A): PubsubMessage =
    PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8(stringFormat.encodeToString(serializer, value)))
      .apply(configure)
      .build()
}

/**
 * KotlinX Serialization implementation of [MessageDecoder]. It's fixed to [StringFormat], so can be
 * re-used for other `string` based serializations.
 */
public class KotlinXJsonDecoder<A>(
  private val stringFormat: StringFormat,
  private val serializer: KSerializer<A>
) : MessageDecoder<A> {
  override suspend fun decode(message: PubsubMessage): A =
    stringFormat.decodeFromString(serializer, message.data.toStringUtf8())
}

/**
 * Allows publishing values of [A], using KotlinX Serialization for Json format.
 *
 * @param topicId which to publish the value
 * @param message that is going to be published
 * @param serializer that is going to be used to serialize [A] to [Json]
 * @param json the KotlinX Serialization [Json] instance to be used
 * @param configure lambda that allows additional configuration to [PubsubMessage.Builder], such as
 *   [PubsubMessage.Builder.setOrderingKey].
 */
public suspend inline fun <reified A> GcpPublisher.publish(
  topicId: TopicId,
  message: A,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: PubsubMessage.Builder.() -> Unit = {}
): String = publish(topicId, message, KotlinXJsonEncoder(json, serializer, configure))

/**
 * Allows publishing values of [Iterable] of [A], using KotlinX Serialization for Json format.
 *
 * @param topicId which to publish the value
 * @param messages that are going to be published
 * @param serializer that is going to be used to serialize [A] to [Json]
 * @param json the KotlinX Serialization [Json] instance to be used
 * @param configure lambda that allows additional configuration to [PubsubMessage.Builder], such as
 *   [PubsubMessage.Builder.setOrderingKey].
 */
public suspend inline fun <reified A> GcpPublisher.publish(
  topicId: TopicId,
  messages: Iterable<A>,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: PubsubMessage.Builder.() -> Unit = {}
): List<String> = publish(topicId, messages, KotlinXJsonEncoder(json, serializer, configure))

/**
 * Allows deserializing [PubsubMessage.getData] from [PubsubRecord] using KotlinX serialization.
 *
 * ```kotlin
 * @Serializable
 * data class Event(val key: String, val message: String)
 *
 * val subscription = SubsriptionId("my-subscription")
 * val subscriber: GcpSubscriber = TODO("Create subscriber")
 * subscriber.subscribe(subscription)
 *   .collect { record: PubsubRecord ->
 *     val event: Event = event.deserialized()
 *     println("event.key: ${event.key}, event.message: ${event.message}")
 *     record.ack()
 *   }
 * ```
 *
 * @param json the KotlinX Serialization [Json] instance to be used
 * @param serializer that is going to be used to serialize [A] to [Json]
 */
public inline fun <reified A> PubsubRecord.deserialized(
  json: Json = Json,
  serializer: KSerializer<A> = serializer()
): A = json.decodeFromString(serializer, data.toStringUtf8())

/**
 * This signature looks the same as the regular [GcpSubscriber.subscribe], but instead takes a
 * generic argument.
 *
 * To use this method you need to explicitly pass the generic argument, otherwise use
 * [subscribeDeserialized]. Alternatively you can also use [deserialized].
 *
 * ```kotlin
 * @Serializable
 * data class Event(val key: String, val message: String)
 *
 * val subscription = SubsriptionId("my-subscription")
 * val subscriber: GcpSubscriber = TODO("Create subscriber")
 * subscriber.subscribe<Event>(subscription)
 *   .collect { (event: Event, record: PubsubRecord) ->
 *     println("event.key: ${event.key}, event.message: ${event.message}")
 *     record.ack()
 *   }
 * ```
 *
 * @see GcpSubscriber.subscribe for full documentation.
 */
public inline fun <reified A> GcpSubscriber.subscribe(
  subscriptionId: SubscriptionId,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: Subscriber.Builder.() -> Unit = {},
): Flow<AcknowledgeableValue<A>> =
  subscribe(subscriptionId, KotlinXJsonDecoder(json, serializer), configure)

/** Alias for subscribe<A>(..) */
public inline fun <reified A> GcpSubscriber.subscribeDeserialized(
  subscriptionId: SubscriptionId,
  json: Json = Json,
  serializer: KSerializer<A> = serializer(),
  noinline configure: Subscriber.Builder.() -> Unit = {},
): Flow<AcknowledgeableValue<A>> = subscribe(subscriptionId, json, serializer, configure)
