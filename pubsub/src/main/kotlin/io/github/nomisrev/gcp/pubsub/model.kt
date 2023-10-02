package io.github.nomisrev.gcp.pubsub

import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName

@JvmInline public value class ProjectId(public val value: String)

@JvmInline public value class SubscriptionId(public val value: String)

@JvmInline public value class TopicId(public val value: String)

public fun projectTopicName(topic: String, projectId: String? = null): ProjectTopicName? =
  // Fully-qualified topic name in the "projects/[project_name]/topics/[topic_name]" format
  if (ProjectTopicName.isParsableFrom(topic)) ProjectTopicName.parse(topic)
  else {
    requireNotNull(projectId) { "The project ID can't be null when using canonical topic name." }
    ProjectTopicName.of(projectId, topic)
  }

public fun topicName(topic: String, projectId: String? = null): TopicName =
  // Fully-qualified topic name in the "projects/[project_name]/topics/[topic_name]" format
  if (TopicName.isParsableFrom(topic)) TopicName.parse(topic)
  else {
    requireNotNull(projectId) { "The project ID can't be null when using canonical topic name." }
    TopicName.of(projectId, topic)
  }

public fun projectSubscriptionName(
  subscription: String,
  projectId: String? = null
): ProjectSubscriptionName =
  // Fully-qualified subscription name in the
  // "projects/[project_name]/subscriptions/[subscription_name]" format
  if (ProjectSubscriptionName.isParsableFrom(subscription))
    ProjectSubscriptionName.parse(subscription)
  else {
    requireNotNull(projectId) {
      "The project ID can't be null when using canonical subscription name."
    }
    ProjectSubscriptionName.of(projectId, subscription)
  }

public fun subscriptionName(subscription: String, projectId: String? = null): SubscriptionName =
  // Fully-qualified subscription name in the
  // "projects/[project_name]/subscriptions/[subscription_name]" format
  if (SubscriptionName.isParsableFrom(subscription)) SubscriptionName.parse(subscription)
  else {
    requireNotNull(projectId) {
      "The project ID can't be null when using canonical subscription name."
    }
    SubscriptionName.of(projectId, subscription)
  }
