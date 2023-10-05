# Module common-api

KotlinX integration module for Google's api-core module. If you need to work with
Google's [`ApiFuture`](https://cloud.google.com/java/docs/reference/api-common/latest/com.google.api.core.ApiFutures).

Extension functions:

| **Name**                 | **Description**                                   
|--------------------------|---------------------------------------------------
| [ApiFuture.await]()      | Awaits for completion of the future (cancellable) 
| [ApiFuture.asDeferred]() | Converts a deferred value to the future           

## Example

Given the following functions defined in some Java API based on Guava:

```java
public ApiFuture<AckResponse> ack(); // starts async acknowledging of message
```

We can consume this API from Kotlin coroutine to load two images and combine then asynchronously.
The resulting function returns `ListenableFuture<Image>` for ease of use back from Guava-based Java code.

```kotlin
suspend fun processMessage(record: PubsubRecord): Unit {
  println(record.message.data.toStringUtf8())
  when (val response = record.ack().await()) {
    SUCCESSFUL -> println("Message was successfully acknowledged. Will not be redelivered.")
    else -> println("Acknowledgment failed, message might be redelivered.")
  }
}
```

Note that this module should be used only for integration with existing Java APIs based on `ApiFuture`.
Writing pure-Kotlin code that uses `ApiFuture` is highly not recommended, since the resulting APIs based
on the futures are quite error-prone. See the discussion on
[Asynchronous Programming Styles](https://github.com/Kotlin/kotlin-coroutines/blob/master/kotlin-coroutines-informal.md#asynchronous-programming-styles)
for details on general problems pertaining to any future-based API and keep in mind that `ApiFuture` exposes
a _blocking_ method [get](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Future.html#get--) that makes
it especially bad choice for coroutine-based Kotlin code.

## Using in your projects

### Gradle

Add dependencies (you can also add other modules that you need):

```kotlin
dependencies {
  implementation("io.github.nomisrev:google-common-api:1.0.0")
}
```

### Maven

Add dependencies (you can also add other modules that you need):

```xml

<dependency>
    <groupId>io.github.nomisrev</groupId>
    <artifactId>google-common-api</artifactId>
    <version>1.0.0</version>
</dependency>
```
