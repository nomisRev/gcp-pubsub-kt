package io.github.nomisrev.gcp.pubsub.ktor

import com.google.api.gax.core.CredentialsProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import io.github.nomisrev.gcp.pubsub.GcpPublisher
import io.github.nomisrev.gcp.pubsub.GcpPubsSubAdmin
import io.github.nomisrev.gcp.pubsub.GcpSubscriber
import io.github.nomisrev.gcp.pubsub.ProjectId
import io.github.nomisrev.gcp.pubsub.PubsubRecord
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import io.github.nomisrev.gcp.pubsub.TopicId
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.BaseApplicationPlugin
import io.ktor.server.application.application
import io.ktor.server.application.install
import io.ktor.server.application.pluginOrNull
import io.ktor.util.AttributeKey
import io.ktor.util.KtorDsl
import io.ktor.util.pipeline.PipelineContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus

public class GcpPubSub(internal val application: Application, configuration: Configuration) {

  internal var configureSubscriber = configuration.configureSubscriber
  internal var configureTopicAdmin: (TopicAdminSettings.Builder.(projectId: ProjectId) -> Unit)? =
    configuration.configureTopicAdmin

  internal var configureSubscriptionAdmin:
    (SubscriptionAdminSettings.Builder.(projectId: ProjectId) -> Unit)? =
    configuration.configureSubscriptionAdmin

  internal var configurePublisher:
    Publisher.Builder.(projectId: ProjectId, topicId: TopicId) -> Unit =
    configuration.configurePublisher

  private val credentialsProvider: CredentialsProvider? = configuration.credentialsProvider
  private val adminCache = ConcurrentHashMap<ProjectId, GcpPubsSubAdmin>()
  private val publisherCache = ConcurrentHashMap<ProjectId, GcpPublisher>()

  internal fun getOrCreateAdmin(projectId: ProjectId): GcpPubsSubAdmin =
    adminCache[projectId]
      ?: adminCache.computeIfAbsent(projectId) {
        GcpPubsSubAdmin(
            projectId,
            TopicAdminClient.create(
              TopicAdminSettings.newBuilder()
                .apply {
                  configureTopicAdmin?.invoke(this, projectId)
                  this@GcpPubSub.credentialsProvider?.let { setCredentialsProvider(it) }
                }
                .build()
            ),
            SubscriptionAdminClient.create(
              SubscriptionAdminSettings.newBuilder()
                .apply {
                  configureSubscriptionAdmin?.invoke(this, projectId)
                  this@GcpPubSub.credentialsProvider?.let { setCredentialsProvider(it) }
                }
                .build()
            )
          )
          .also { admin ->
            application.environment.monitor.subscribe(ApplicationStopped) { admin.close() }
          }
      }

  internal fun getOrCreatePublisher(projectId: ProjectId): GcpPublisher =
    publisherCache[projectId]
      ?: publisherCache.computeIfAbsent(projectId) {
        GcpPublisher(projectId) { configurePublisher(projectId, it) }
          .also { publisher ->
            application.environment.monitor.subscribe(ApplicationStopped) { publisher.close() }
          }
      }

  public fun admin(projectId: ProjectId): GcpPubsSubAdmin = getOrCreateAdmin(projectId)

  public fun publisher(projectId: ProjectId): GcpPublisher = getOrCreatePublisher(projectId)

  @OptIn(ExperimentalCoroutinesApi::class)
  internal fun gcpPubSub(projectId: ProjectId, scope: CoroutineScope): GcpPubSubSyntax =
    object :
      GcpPubSubSyntax,
      GcpPubsSubAdmin by getOrCreateAdmin(projectId),
      GcpPublisher by getOrCreatePublisher(projectId),
      CoroutineScope by scope {
      override val subscriber = GcpSubscriber(projectId, configureSubscriber)

      override fun subscribe(
        subscriptionId: SubscriptionId,
        concurrency: Int,
        context: CoroutineContext,
        configure: Subscriber.Builder.() -> Unit,
        handler: suspend (PubsubRecord) -> Unit
      ): Job =
        subscriber
          .subscribe(subscriptionId) { configure() }
          .map { record -> flow { emit(handler(record)) } }
          .flattenMerge(concurrency)
          .launchIn(scope + context)

      // These are managed by application.environment.monitor.subscribe(ApplicationStopped)
      override fun close() {}
    }

  @KtorDsl
  public class Configuration {
    public var credentialsProvider: CredentialsProvider? = null
    internal var configureSubscriber: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit =
      {}
    internal var configurePublisher:
      Publisher.Builder.(projectId: ProjectId, topicId: TopicId) -> Unit =
      { _, _ ->
      }
    internal var configureTopicAdmin: (TopicAdminSettings.Builder.(projectId: ProjectId) -> Unit)? =
      null
    internal var configureSubscriptionAdmin:
      (SubscriptionAdminSettings.Builder.(projectId: ProjectId) -> Unit)? =
      null

    /**
     * Global [Subscriber.Builder] configuration that will be applied to all [Subscriber]'s created
     * by the [GcpPubSub] plugin. You can provide additional configuration on a per [subscriber]
     * basis.
     */
    public fun subscriber(block: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit) {
      configureSubscriber = block
    }

    /**
     * Global [Publisher.Builder] configuration that will be applied to all [Publisher]'s created by
     * the [GcpPubSub] plugin. It's not possible to provide additional configuration on a
     * per-publisher basis, since publishers are cached under the hood for performance reasons.
     *
     * If you need to add configuration for a specific [Publisher] do so by checking `projectId` and
     * `topicId` parameters of the lambda.
     */
    public fun publisher(
      block: Publisher.Builder.(projectId: ProjectId, topicId: TopicId) -> Unit
    ) {
      configurePublisher = block
    }

    /**
     * Global [TopicAdminSettings.Builder] configuration that will be applied to all
     * [TopicAdminClient]'s created by the [GcpPubSub] plugin. It's not possible to provide
     * additional configuration on a per-publisher basis, since publishers are cached under the hood
     * for performance reasons.
     *
     * If you need to add configuration for a specific [TopicAdminClient] do so by checking
     * `projectId` parameters of the lambda.
     */
    public fun topicAdmin(block: TopicAdminSettings.Builder.(projectId: ProjectId) -> Unit) {
      configureTopicAdmin = block
    }

    /**
     * Global [SubscriptionAdminSettings.Builder] configuration that will be applied to all
     * [SubscriptionAdminClient]'s created by the [GcpPubSub] plugin. It's not possible to provide
     * additional configuration on a per-publisher basis, since publishers are cached under the hood
     * for performance reasons.
     *
     * If you need to add configuration for a specific [SubscriptionAdminClient] do so by checking
     * `projectId` parameters of the lambda.
     */
    public fun subscriptionAdmin(
      block: SubscriptionAdminSettings.Builder.(projectId: ProjectId) -> Unit
    ) {
      configureSubscriptionAdmin = block
    }
  }

  public companion object Plugin : BaseApplicationPlugin<Application, Configuration, GcpPubSub> {
    override val key: AttributeKey<GcpPubSub> = AttributeKey("GcpPubSubPlugin")

    public override fun install(
      pipeline: Application,
      configure: Configuration.() -> Unit
    ): GcpPubSub = GcpPubSub(pipeline, Configuration().apply(configure))
  }
}

/**
 * Retrieve the [GcpPubSub] plugin, and launch a coroutine that creates a Gcp PubSub based program
 * using [GcpPubSubSyntax]. It gives access to all functions from [GcpPubsSubAdmin] &
 * [GcpPublisher].
 *
 * ```kotlin
 * fun Application.process(): Job =
 *   pubSub(projectId) {
 *     // Access all GcpPubSubAdmin functions
 *     createTopic(TopicId("my-topic"))
 *     createSubscription(SubscriptionId("my-subscription"), TopicId("my-topic"))
 *
 *     // Access to all GcpPublisher functions
 *     publish(TopicId("my-topic"), listOf("my-message1", "my-message2"))
 *
 *     subscribe(SubscriptionId("my-subscription")) { record ->
 *       println(record.message.data.toStringUtf8())
 *       record.ack().
 *     }
 *   }
 * ```
 */
public fun Application.pubSub(
  projectId: ProjectId,
  block: suspend GcpPubSubSyntax.() -> Unit
): Job {
  val plugin = pubSub()
  return launch { block(plugin.gcpPubSub(projectId, this)) }
}

/**
 * Retrieve the GcpPubSub plugin, this allows access to the cached [GcpPubsSubAdmin] &
 * [GcpPublisher] instances
 *
 * ```kotlin
 * fun Application.route(): Routing =
 *   routing {
 *     post("/publish") {
 *       pubSub().publisher(ProjectId("my-project"))
 *         .publish(TopicId("my-topic"), "my-message")
 *       call.respond(HttpStatusCode.Accepted)
 *     }
 *
 *     post("/delete/{topic}") {
 *       val topic =
 *         requireNotNull(call.parameters["topic"]) { "Missing parameter topic" }
 *       pubSub().admin(projectId).deleteTopic(TopicId(topic))
 *     }
 *   }
 * ```
 */
public fun PipelineContext<Unit, ApplicationCall>.pubSub(): GcpPubSub = application.pubSub()

private fun Application.pubSub(): GcpPubSub = pluginOrNull(GcpPubSub) ?: install(GcpPubSub)
