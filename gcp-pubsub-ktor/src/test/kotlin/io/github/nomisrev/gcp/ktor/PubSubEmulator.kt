package io.github.nomisrev.pubsub

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
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
import io.github.nomisrev.gcp.pubsub.SubscriptionId
import io.github.nomisrev.gcp.pubsub.TopicId
import io.github.nomisrev.gcp.pubsub.ktor.GcpPubSub
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.kotest.core.extensions.MountableExtension
import io.kotest.core.listeners.AfterProjectListener
import io.kotest.core.listeners.AfterSpecListener
import io.ktor.server.application.Application
import io.ktor.server.application.BaseApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.application.pluginOrNull
import io.ktor.util.AttributeKey
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName

class PubSubEmulator(
  imageName: DockerImageName =
    DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators"),
  private val credentials: CredentialsProvider = NoCredentialsProvider.create()
) :
  MountableExtension<PubSubEmulatorContainer, PubSubEmulator>,
  AfterProjectListener,
  AfterSpecListener,
  AutoCloseable,
  BaseApplicationPlugin<Application, Unit, Unit> {

  override val key: AttributeKey<Unit> = AttributeKey("PubSubEmulatorExtension")

  override fun install(pipeline: Application, configure: Unit.() -> Unit) {
    val plugin = (pipeline.pluginOrNull(GcpPubSub) ?: pipeline.install(GcpPubSub))
    val configurePublisher = plugin.configurePublisher
    plugin.configurePublisher = { projectId, topicId ->
      configurePublisher(projectId, topicId)
      setChannelProvider(channel)
      setCredentialsProvider(credentials)
    }
    val configureSubscriber = plugin.configureSubscriber
    plugin.configureSubscriber = {
      configureSubscriber(it)
      setChannelProvider(channel)
      setCredentialsProvider(credentials)
    }
    val configureTopicAdmin = plugin.configureTopicAdmin
    plugin.configureTopicAdmin = {
      configureTopicAdmin?.invoke(this, it)
      setTransportChannelProvider(channel)
      setCredentialsProvider(credentials)
    }
    val configureSubscriptionAdmin = plugin.configureSubscriptionAdmin
    plugin.configureSubscriptionAdmin = {
      configureSubscriptionAdmin?.invoke(this, it)
      setTransportChannelProvider(channel)
      setCredentialsProvider(credentials)
    }
  }

  private val container = PubSubEmulatorContainer(imageName)

  private val managedChannel: ManagedChannel by lazy {
    ManagedChannelBuilder.forTarget(container.emulatorEndpoint).usePlaintext().build()
  }

  val channel: FixedTransportChannelProvider by lazy {
    FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel))
  }

  /** Create [TopicAdminSettings] that is by default linked to test channel. */
  fun topicAdminSettings(
    transportChannelProvider: TransportChannelProvider = channel,
    credentialsProvider: CredentialsProvider = credentials
  ): TopicAdminSettings =
    TopicAdminSettings.newBuilder()
      .setTransportChannelProvider(transportChannelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()

  /** Create [SubscriptionAdminSettings] that is by default linked to test channel. */
  fun subscriptionAdminSettings(
    transportChannelProvider: TransportChannelProvider = channel,
    credentialsProvider: CredentialsProvider = credentials
  ): SubscriptionAdminSettings =
    SubscriptionAdminSettings.newBuilder()
      .setTransportChannelProvider(transportChannelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()

  /** Generate a unique topic name */
  fun uniqueTopic(): TopicId = TopicId("topic-${UUID.randomUUID()}")

  /** Generate a unique subscription name */
  fun uniqueSubscription(): SubscriptionId = SubscriptionId("subscription-${UUID.randomUUID()}")

  fun subscriber(
    projectId: ProjectId,
    configure: Subscriber.Builder.(subscriptionId: SubscriptionId) -> Unit = {}
  ): GcpSubscriber =
    GcpSubscriber(projectId) {
      configure(it)
      setChannelProvider(channel)
      setCredentialsProvider(credentials)
    }

  fun publisher(
    projectId: ProjectId,
    configure: Publisher.Builder.(topicId: TopicId) -> Unit = {}
  ): GcpPublisher =
    GcpPublisher(projectId) {
      configure(it)
      setChannelProvider(channel)
      setCredentialsProvider(credentials)
    }

  fun admin(projectId: ProjectId): GcpPubsSubAdmin =
    GcpPubsSubAdmin(
      projectId,
      TopicAdminClient.create(
        TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(channel)
          .setCredentialsProvider(credentials)
          .build()
      ),
      SubscriptionAdminClient.create(
        SubscriptionAdminSettings.newBuilder()
          .setTransportChannelProvider(channel)
          .setCredentialsProvider(credentials)
          .build()
      )
    )

  override fun mount(configure: PubSubEmulatorContainer.() -> Unit): PubSubEmulator {
    if (!container.isRunning) {
      container.configure()
      container.start()
    }
    return this
  }

  override fun close() {
    managedChannel.shutdown()
    managedChannel.awaitTermination(5, TimeUnit.SECONDS)
    container.stop()
  }

  override suspend fun afterProject() {
    managedChannel.shutdown()
    managedChannel.awaitTermination(5, TimeUnit.SECONDS)
    container.stop()
  }
}
