package io.github.nomisrev.gcp.pubsub

import com.google.api.gax.rpc.AlreadyExistsException
import com.google.api.gax.rpc.ApiException
import com.google.api.gax.rpc.InvalidArgumentException
import com.google.api.gax.rpc.NotFoundException
import com.google.api.gax.rpc.StatusCode
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.pubsub.v1.DeleteSubscriptionRequest
import com.google.pubsub.v1.DeleteTopicRequest
import com.google.pubsub.v1.GetSubscriptionRequest
import com.google.pubsub.v1.GetTopicRequest
import com.google.pubsub.v1.ListSubscriptionsRequest
import com.google.pubsub.v1.ListTopicsRequest
import com.google.pubsub.v1.Subscription
import com.google.pubsub.v1.Topic
import io.github.nomisrev.gcp.core.await

public fun GcpPubsSubAdmin(
  projectId: ProjectId,
  configureSubscriptionAdmin: SubscriptionAdminSettings.Builder.() -> Unit,
  configureTopicAdmin: TopicAdminSettings.Builder.() -> Unit,
): GcpPubsSubAdmin {
  val topicAdminClient =
    TopicAdminClient.create(TopicAdminSettings.newBuilder().apply(configureTopicAdmin).build())

  val subscriptionAdminClient =
    try {
      SubscriptionAdminClient.create(
        SubscriptionAdminSettings.newBuilder().apply(configureSubscriptionAdmin).build()
      )
    } catch (ex: Exception) {
      topicAdminClient.close()
      throw ex
    }
  return DefaultPubSubAdmin(projectId, topicAdminClient, subscriptionAdminClient)
}

public fun GcpPubsSubAdmin(
  projectId: ProjectId,
  topicAdminClient: TopicAdminClient,
  subscriptionAdminClient: SubscriptionAdminClient,
): GcpPubsSubAdmin {
  require(projectId.value.isNotBlank()) { "The project ID can't be null or empty." }
  return DefaultPubSubAdmin(projectId, topicAdminClient, subscriptionAdminClient)
}

public interface GcpPubsSubAdmin : AutoCloseable {
  /**
   * Creates the given topic with the given name. See the
   * [resource name rules](https://cloud.google.com/pubsub/docs/admin#resource_names).
   *
   * @param topicId The name of the topic. `{topic}` must start with a letter, and contain only
   *   letters (`[A-Za-z]`), numbers (`[0-9]`), dashes (`-`), underscores (`_`), periods (`.`),
   *   tildes (`~`), plus (`+`) or percent signs (`%`). It must be between 3 and 255 characters in
   *   length, and it must not start with `"goog"`.
   * @throws [InvalidArgumentException] when topic doesn't follow correct naming requirements
   * @throws [AlreadyExistsException] when topic already exists
   * @throws [ApiException] if the remote call fails
   * @see TopicAdminClient.createTopic
   */
  public suspend fun createTopic(topicId: TopicId): Topic

  /**
   * Deletes the topic with the given name. After a topic is deleted, a new topic may be created
   * with the same name; this is an entirely new topic with none of the old configuration or
   * subscriptions. Existing subscriptions to this topic are not deleted, but their `topic` field is
   * set to `_deleted-topic_`.
   *
   * @param topicId the name of the topic to be deleted
   * @throws [InvalidArgumentException] when empty topic name is specified
   * @throws [NotFoundException] when topic not found
   */
  public suspend fun deleteTopic(topicId: TopicId)

  public suspend fun getTopic(topicId: TopicId): Topic?

  public suspend fun listTopics(): List<Topic>

  /**
   * Creates a subscription to a given topic. See the
   * [resource name rules](https://cloud.google.com/pubsub/docs/admin#resource_names)
   *
   * If the name is not provided in the request, the server will assign a random name for this
   * subscription on the same project as the topic, conforming to the [resource name format]
   * (https://cloud.google.com/pubsub/docs/admin#resource_names). The generated name is populated in
   * the returned Subscription object. Note that for REST API requests, you must specify a name in
   * the request.
   *
   * @param configure the [Subscription] with configuration such as `ackDeadline`, `pushConfig`,
   *   `messageOrdering`, dead lettering, etc.
   * @throws [InvalidArgumentException] when invalid subscription name is specified
   * @throws [AlreadyExistsException] when subscription already exists
   * @throws [NotFoundException] if the [topicId] doesn't exist
   */
  public suspend fun createSubscription(
    subscriptionId: SubscriptionId,
    topicId: TopicId,
    configure: Subscription.Builder.() -> Unit = {},
  ): Subscription

  /** Throws [NotFoundException] when subscription not found */
  public suspend fun deleteSubscription(subscriptionId: SubscriptionId)

  /**
   * Get the configuration of a Google Cloud Pub/Sub subscription.
   *
   * @param subscriptionId short subscription name, e.g., "subscriptionId", or the fully-qualified
   *   subscription name in the `projects/{project_name}/subscriptions/{subscription_name}` format
   * @return subscription configuration or `null` if subscription doesn't exist
   */
  public suspend fun getSubscription(subscriptionId: SubscriptionId): Subscription?

  public suspend fun listSubscriptions(): List<Subscription>
}

private class DefaultPubSubAdmin(
  val projectId: ProjectId,
  val topicAdminClient: TopicAdminClient,
  val subscriptionAdminClient: SubscriptionAdminClient,
) : GcpPubsSubAdmin {

  override suspend fun createTopic(topicId: TopicId): Topic {
    return topicAdminClient
      .createTopicCallable()
      .futureCall(Topic.newBuilder().setName(topicId.toTopicName(projectId).toString()).build())
      .await()
  }

  override suspend fun deleteTopic(topicId: TopicId) {
    topicAdminClient
      .deleteTopicCallable()
      .futureCall(
        DeleteTopicRequest.newBuilder().setTopic(topicId.toTopicName(projectId).toString()).build()
      )
      .await()
  }

  override suspend fun getTopic(topicId: TopicId): Topic? {
    require(topicId.value.isNotBlank()) { "No topic name was specified." }
    return try {
      topicAdminClient.topicCallable
        .futureCall(
          GetTopicRequest.newBuilder().setTopic(topicId.toTopicName(projectId).toString()).build()
        )
        .await()
    } catch (aex: ApiException) {
      if (aex.statusCode.code == StatusCode.Code.NOT_FOUND) null else throw aex
    }
  }

  override suspend fun listTopics(): List<Topic> =
    topicAdminClient
      .listTopicsCallable()
      .futureCall(ListTopicsRequest.newBuilder().setProject(projectId.value).build())
      .await()
      .topicsList

  override suspend fun createSubscription(
    subscriptionId: SubscriptionId,
    topicId: TopicId,
    configure: Subscription.Builder.() -> Unit,
  ): Subscription =
    subscriptionAdminClient
      .createSubscriptionCallable()
      .futureCall(
        Subscription.newBuilder()
          .setTopic(topicId.toTopicName(projectId).toString())
          .setName(subscriptionId.toSubscriptionName(projectId).toString())
          .apply(configure)
          .build()
      )
      .await()

  override suspend fun deleteSubscription(subscriptionId: SubscriptionId) {
    subscriptionAdminClient
      .deleteSubscriptionCallable()
      .futureCall(
        DeleteSubscriptionRequest.newBuilder()
          .setSubscription(subscriptionId.toSubscriptionName(projectId).toString())
          .build()
      )
      .await()
  }

  override suspend fun getSubscription(subscriptionId: SubscriptionId): Subscription? {
    require(subscriptionId.value.isNotEmpty()) { "No subscription name was specified" }
    return try {
      subscriptionAdminClient.subscriptionCallable
        .futureCall(
          GetSubscriptionRequest.newBuilder()
            .setSubscription(subscriptionId.toSubscriptionName(projectId).toString())
            .build()
        )
        .await()
    } catch (aex: ApiException) {
      if (aex.statusCode.code == StatusCode.Code.NOT_FOUND) null else throw aex
    }
  }

  override suspend fun listSubscriptions(): List<Subscription> =
    subscriptionAdminClient
      .listSubscriptionsCallable()
      .futureCall(ListSubscriptionsRequest.getDefaultInstance())
      .await()
      .subscriptionsList

  override fun close() {
    topicAdminClient.close()
    subscriptionAdminClient.close()
  }
}
