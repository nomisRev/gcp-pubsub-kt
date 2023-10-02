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
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.BaseApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.application.pluginOrNull
import io.ktor.util.AttributeKey
import io.ktor.util.KtorDsl
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
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

  @OptIn(ExperimentalCoroutinesApi::class)
  internal fun gcpPubSub(projectId: ProjectId): GcpPubSubSyntax =
    object :
      GcpPubSubSyntax,
      GcpPubsSubAdmin by getOrCreateAdmin(projectId),
      GcpPublisher by getOrCreatePublisher(projectId) {
      val subscriber = GcpSubscriber(projectId, configureSubscriber)

      override fun subscribe(
        subscriptionId: SubscriptionId,
        concurrency: Int,
        context: CoroutineContext,
        configure: Subscriber.Builder.() -> Unit,
        handler: suspend (PubsubRecord) -> Unit
      ): Job =
        subscriber
          .subscribe(subscriptionId) {
            configureSubscriber(subscriptionId)
            configure()
          }
          .map { record -> flow { emit(handler(record)) } }
          .flattenMerge(concurrency)
          .launchIn(application + context)

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

    public fun subscriber(block: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit) {
      configureSubscriber = block
    }

    public fun publisher(
      block: Publisher.Builder.(projectId: ProjectId, topicId: TopicId) -> Unit
    ) {
      configurePublisher = block
    }

    public fun topicAdmin(block: TopicAdminSettings.Builder.(projectId: ProjectId) -> Unit) {
      configureTopicAdmin = block
    }

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
    ): GcpPubSub {
      val config = Configuration().apply(configure)
      return GcpPubSub(pipeline, config)
    }
  }
}

public fun Application.pubSub(
  projectId: ProjectId,
  block: suspend GcpPubSubSyntax.() -> Unit
): Job {
  val plugin = pluginOrNull(GcpPubSub) ?: install(GcpPubSub)
  return launch { block(plugin.gcpPubSub(projectId)) }
}
