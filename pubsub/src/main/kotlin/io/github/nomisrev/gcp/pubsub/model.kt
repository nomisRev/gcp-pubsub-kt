package io.github.nomisrev.gcp.pubsub

import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse
import com.google.protobuf.Empty
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PubsubMessageOrBuilder
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import kotlinx.coroutines.Deferred

@JvmInline public value class ProjectId(public val value: String)

@JvmInline
public value class SubscriptionId(public val value: String) {
  public fun toProjectSubscriptionName(projectId: ProjectId?): ProjectSubscriptionName =
    // Fully-qualified subscription name in the
    // "projects/[project_name]/subscriptions/[subscription_name]" format
    if (ProjectSubscriptionName.isParsableFrom(value)) ProjectSubscriptionName.parse(value)
    else {
      requireNotNull(projectId) {
        "The project ID can't be null when using canonical subscription name."
      }
      ProjectSubscriptionName.of(projectId.value, value)
    }

  public fun toSubscriptionName(projectId: ProjectId?): SubscriptionName =
    // Fully-qualified subscription name in the
    // "projects/[project_name]/subscriptions/[subscription_name]" format
    if (SubscriptionName.isParsableFrom(value)) SubscriptionName.parse(value)
    else {
      requireNotNull(projectId) {
        "The project ID can't be null when using canonical subscription name."
      }
      SubscriptionName.of(projectId.value, value)
    }
}

@JvmInline
public value class TopicId(public val value: String) {
  public fun toProjectTopicName(projectId: ProjectId?): ProjectTopicName =
    // Fully-qualified topic name in the "projects/[project_name]/topics/[topic_name]" format
    if (ProjectTopicName.isParsableFrom(value)) ProjectTopicName.parse(value)
    else {
      requireNotNull(projectId) { "The project ID can't be null when using canonical topic name." }
      ProjectTopicName.of(projectId.value, value)
    }

  public fun toTopicName(projectId: ProjectId?): TopicName =
    // Fully-qualified topic name in the "projects/[project_name]/topics/[topic_name]" format
    if (TopicName.isParsableFrom(value)) TopicName.parse(value)
    else {
      requireNotNull(projectId) { "The project ID can't be null when using canonical topic name." }
      TopicName.of(projectId.value, value)
    }
}

public class PubsubRecord(
  public val message: PubsubMessage,
  private val consumer: AckReplyConsumerWithResponse,
  public val projectId: ProjectId,
  public val subscriptionId: SubscriptionId,
) : PubsubMessageOrBuilder by message, AckReplyConsumerWithResponse by consumer

public data class AcknowledgeableValue<A>(val value: A, val record: PubsubRecord)

public interface AckPubSubMessage : PubsubMessageOrBuilder {

  public val projectId: ProjectId

  public val subscriptionId: SubscriptionId

  public val pubSubMessage: PubsubMessage

  public val ackId: String

  public fun ack(): Deferred<Empty>

  /**
   * Modify the ack deadline of the message. Once the ack deadline expires, the message is
   * automatically nacked.
   *
   * @param ackDeadlineSeconds the new ack deadline in seconds A deadline of 0 effectively nacks the
   *   message.
   */
  public fun modifyAckDeadline(ackDeadlineSeconds: Int): Deferred<Empty>

  public fun nack(): Deferred<Empty> = modifyAckDeadline(0)
}
