package io.github.nomisrev.gcp.pubsub

import com.google.api.core.ApiFutures
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.github.nomisrev.gcp.core.await
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

public fun GcpPublisher(
  projectId: ProjectId,
  configure: Publisher.Builder.(topicId: TopicId) -> Unit
): GcpPublisher = DefaultGcpPublisher(projectId, configure)

public interface MessageEncoder<A> {
  public suspend fun encode(value: A): PubsubMessage
}

public interface GcpPublisher : AutoCloseable {
  public suspend fun publish(topicId: TopicId, message: PubsubMessage): String

  public suspend fun publish(topicId: TopicId, messages: Iterable<PubsubMessage>): List<String>

  public suspend fun <A> publish(
    topicId: TopicId,
    messages: Iterable<A>,
    encoder: MessageEncoder<A>
  ): List<String> = publish(topicId, messages.map { encoder.encode(it) })

  public suspend fun publish(
    topicId: TopicId,
    message: ByteString,
    configure: PubsubMessage.Builder.() -> Unit = {}
  ): String = publish(topicId, PubsubMessage.newBuilder().setData(message).apply(configure).build())

  public suspend fun publish(
    topicId: TopicId,
    message: String,
    configure: PubsubMessage.Builder.() -> Unit = {}
  ): String =
    publish(
      topicId,
      PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).apply(configure).build()
    )

  public suspend fun publish(
    topicId: TopicId,
    message: ByteBuffer,
    configure: PubsubMessage.Builder.() -> Unit = {}
  ): String =
    publish(
      topicId,
      PubsubMessage.newBuilder().setData(ByteString.copyFrom(message)).apply(configure).build()
    )

  public suspend fun publish(
    topicId: TopicId,
    message: ByteArray,
    configure: PubsubMessage.Builder.() -> Unit = {}
  ): String =
    publish(
      topicId,
      PubsubMessage.newBuilder().setData(ByteString.copyFrom(message)).apply(configure).build()
    )

  public suspend fun <A> publish(topicId: TopicId, message: A, encoder: MessageEncoder<A>): String =
    publish(topicId, encoder.encode(message))
}

@JvmName("publishByteString")
public suspend fun GcpPublisher.publish(
  topicId: TopicId,
  messages: Iterable<ByteString>,
  configure: PubsubMessage.Builder.() -> Unit = {}
): List<String> =
  publish(topicId, messages.map { PubsubMessage.newBuilder().setData(it).apply(configure).build() })

@JvmName("publishString")
public suspend fun GcpPublisher.publish(
  topicId: TopicId,
  messages: Iterable<String>,
  configure: PubsubMessage.Builder.() -> Unit = {}
): List<String> =
  publish(
    topicId,
    messages.map {
      PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(it)).apply(configure).build()
    }
  )

@JvmName("publishByteBuffer")
public suspend fun GcpPublisher.publish(
  topicId: TopicId,
  messages: Iterable<ByteBuffer>,
  configure: PubsubMessage.Builder.() -> Unit = {}
): List<String> =
  publish(
    topicId,
    messages.map {
      PubsubMessage.newBuilder().setData(ByteString.copyFrom(it)).apply(configure).build()
    }
  )

@JvmName("publishByteArray")
public suspend fun GcpPublisher.publish(
  topicId: TopicId,
  messages: Iterable<ByteArray>,
  configure: PubsubMessage.Builder.() -> Unit = {}
): List<String> =
  publish(
    topicId,
    messages.map {
      PubsubMessage.newBuilder().setData(ByteString.copyFrom(it)).apply(configure).build()
    }
  )

private class DefaultGcpPublisher(
  val projectId: ProjectId,
  val configure: Publisher.Builder.(topicId: TopicId) -> Unit
) : GcpPublisher {
  val publisherCache = ConcurrentHashMap<TopicId, Publisher>()

  override suspend fun publish(topicId: TopicId, message: PubsubMessage): String =
    getOrCreatePublisher(topicId).publish(message).await()

  override suspend fun publish(topicId: TopicId, messages: Iterable<PubsubMessage>): List<String> {
    val publisher = getOrCreatePublisher(topicId)
    return ApiFutures.allAsList(messages.map(publisher::publish)).await()
  }

  override fun close() {
    publisherCache.forEachValue(1, Publisher::shutdown)
  }

  fun getOrCreatePublisher(topicId: TopicId): Publisher =
    publisherCache[topicId]
      ?: publisherCache.computeIfAbsent(topicId) {
        Publisher.newBuilder(projectTopicName(topicId.value, projectId.value))
          .apply { configure(topicId) }
          .build()
      }
}
