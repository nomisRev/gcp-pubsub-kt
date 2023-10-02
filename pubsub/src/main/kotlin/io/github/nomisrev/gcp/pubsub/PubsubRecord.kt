package io.github.nomisrev.gcp.pubsub

import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PubsubMessageOrBuilder

public class PubsubRecord(
  public val message: PubsubMessage,
  private val consumer: AckReplyConsumerWithResponse,
  public val projectId: ProjectId,
  public val subscriptionId: SubscriptionId,
) : PubsubMessageOrBuilder by message, AckReplyConsumerWithResponse by consumer
