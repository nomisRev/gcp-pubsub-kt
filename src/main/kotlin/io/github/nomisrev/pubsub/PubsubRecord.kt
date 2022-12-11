package io.github.nomisrev.pubsub

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PubsubMessageOrBuilder

class PubsubRecord(
  private val pubsubMessage: PubsubMessage,
  private val consumer: AckReplyConsumer
) : PubsubMessageOrBuilder by pubsubMessage {
  fun ack() = consumer.ack()
  fun nack() = consumer.ack()
}
