package io.github.nomisrev

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminSettings
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit
import org.junit.rules.ExternalResource
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName

public class PubSubEmulator : ExternalResource() {

  public val container: PubSubEmulatorContainer =
    PubSubEmulatorContainer(
      DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators")
    )

  private val managedChannel =
    ManagedChannelBuilder.forTarget(container.emulatorEndpoint).usePlaintext().build()

  public val channel: FixedTransportChannelProvider =
    FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel))

  public fun topicSettings(
    channel: TransportChannelProvider,
    credentialsProvider: CredentialsProvider
  ): TopicAdminSettings =
    TopicAdminSettings.newBuilder()
      .setTransportChannelProvider(channel)
      .setCredentialsProvider(credentialsProvider)
      .build()

  public fun subSettings(
    channel: TransportChannelProvider,
    credentials: CredentialsProvider
  ): SubscriptionAdminSettings =
    SubscriptionAdminSettings.newBuilder()
      .setTransportChannelProvider(channel)
      .setCredentialsProvider(credentials)
      .build()

  override fun before() {
    super.before()
    container.start()
  }

  override fun after() {
    super.after()
    managedChannel.shutdown()
    managedChannel.awaitTermination(5, TimeUnit.SECONDS)
    container.stop()
  }
}
