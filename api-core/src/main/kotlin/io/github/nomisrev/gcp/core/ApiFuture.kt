/**
 * This file is inspired by
 * https://github.com/Kotlin/kotlinx.coroutines/blob/master/integration/kotlinx-coroutines-guava/src/ListenableFuture.kt
 */
package io.github.nomisrev.gcp.core

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.Uninterruptibles
import com.google.common.util.concurrent.internal.InternalFutureFailureAccess
import com.google.common.util.concurrent.internal.InternalFutures
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.AbstractCoroutine
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.handleCoroutineException
import kotlinx.coroutines.suspendCancellableCoroutine

/**
 * Returns a [Deferred] that is completed or failed by `this` [ListenableFuture].
 *
 * Completion is non-atomic between the two promises.
 *
 * Cancellation is propagated bidirectionally.
 *
 * When `this` `ListenableFuture` completes (either successfully or exceptionally) it will try to
 * complete the returned `Deferred` with the same value or exception. This will succeed, barring a
 * race with cancellation of the `Deferred`.
 *
 * When `this` `ListenableFuture` is [successfully cancelled][java.util.concurrent.Future.cancel],
 * it will cancel the returned `Deferred`.
 *
 * When the returned `Deferred` is [cancelled][Deferred.cancel], it will try to propagate the
 * cancellation to `this` `ListenableFuture`. Propagation will succeed, barring a race with the
 * `ListenableFuture` completing normally. This is the only case in which the returned `Deferred`
 * will complete with a different outcome than `this` `ListenableFuture`.
 */
public fun <T> ApiFuture<T>.asDeferred(): Deferred<T> {
  /* This method creates very specific behaviour as it entangles the `Deferred` and
   * `ListenableFuture`. This behaviour is the best discovered compromise between the possible
   * states and interface contracts of a `Future` and the states of a `Deferred`. The specific
   * behaviour is described here.
   *
   * When `this` `ListenableFuture` is successfully cancelled - meaning
   * `ListenableFuture.cancel()` returned `true` - it will synchronously cancel the returned
   * `Deferred`. This can only race with cancellation of the returned `Deferred`, so the
   * `Deferred` will always be put into its "cancelling" state and (barring uncooperative
   * cancellation) _eventually_ reach its "cancelled" state when either promise is successfully
   * cancelled.
   *
   * When the returned `Deferred` is cancelled, `ListenableFuture.cancel()` will be synchronously
   * called on `this` `ListenableFuture`. This will attempt to cancel the `Future`, though
   * cancellation may not succeed and the `ListenableFuture` may complete in a non-cancelled
   * terminal state.
   *
   * The returned `Deferred` may receive and suppress the `true` return value from
   * `ListenableFuture.cancel()` when the task is cancelled via the `Deferred` reference to it.
   * This is unavoidable, so make sure no idempotent cancellation work is performed by a
   * reference-holder of the `ListenableFuture` task. The idempotent work won't get done if
   * cancellation was from the `Deferred` representation of the task.
   *
   * This is inherently a race. See `Future.cancel()` for a description of `Future` cancellation
   * semantics. See `Job` for a description of coroutine cancellation semantics.
   */
  // First, try the fast-fast error path for Guava ListenableFutures. This will save allocating an
  // Exception by using the same instance the Future created.
  if (this is InternalFutureFailureAccess) {
    val t: Throwable? = InternalFutures.tryInternalFastPathGetFailure(this)
    if (t != null) {
      return CompletableDeferred<T>().also { it.completeExceptionally(t) }
    }
  }

  // Second, try the fast path for a completed Future. The Future is known to be done, so get()
  // will not block, and thus it won't be interrupted. Calling getUninterruptibly() instead of
  // getDone() in this known-non-interruptible case saves the volatile read that getDone() uses to
  // handle interruption.
  if (isDone) {
    return try {
      CompletableDeferred(Uninterruptibles.getUninterruptibly(this))
    } catch (e: CancellationException) {
      CompletableDeferred<T>().also { it.cancel(e) }
    } catch (e: ExecutionException) {
      // ExecutionException is the only kind of exception that can be thrown from a gotten
      // Future. Anything else showing up here indicates a very fundamental bug in a
      // Future implementation.
      CompletableDeferred<T>().also { it.completeExceptionally(e.nonNullCause()) }
    }
  }

  // Finally, if this isn't done yet, attach a Listener that will complete the Deferred.
  val deferred = CompletableDeferred<T>()
  @OptIn(InternalCoroutinesApi::class)
  ApiFutures.addCallback(
    this,
    object : ApiFutureCallback<T> {
      override fun onSuccess(result: T) {
        runCatching { deferred.complete(result) }
          .onFailure { handleCoroutineException(EmptyCoroutineContext, it) }
      }

      override fun onFailure(t: Throwable) {
        runCatching { deferred.completeExceptionally(t) }
          .onFailure { handleCoroutineException(EmptyCoroutineContext, it) }
      }
    },
    MoreExecutors.directExecutor()
  )

  // ... And cancel the Future when the deferred completes. Since the return type of this method
  // is Deferred, the only interaction point from the caller is to cancel the Deferred. If this
  // completion handler runs before the Future is completed, the Deferred must have been
  // cancelled and should propagate its cancellation. If it runs after the Future is completed,
  // this is a no-op.
  deferred.invokeOnCompletion { cancel(false) }
  // Return hides the CompletableDeferred. This should prevent casting.
  return object : Deferred<T> by deferred {}
}

/**
 * Awaits completion of `this` [ApiFuture] without blocking a thread.
 *
 * This suspend function is cancellable.
 *
 * If the [Job] of the current coroutine is cancelled or completed while this suspending function is
 * waiting, this function stops waiting for the future and immediately resumes with
 * [CancellationException][kotlinx.coroutines.CancellationException].
 *
 * This method is intended to be used with one-shot Futures, so on coroutine cancellation, the
 * Future is cancelled as well. If cancelling the given future is undesired, use
 * [Futures.nonCancellationPropagating] or [kotlinx.coroutines.NonCancellable].
 */
public suspend fun <T> ApiFuture<T>.await(): T {
  try {
    if (isDone) return Uninterruptibles.getUninterruptibly(this)
  } catch (e: ExecutionException) {
    // ExecutionException is the only kind of exception that can be thrown from a gotten
    // Future, other than CancellationException. Cancellation is propagated upward so that
    // the coroutine running this suspend function may process it.
    // Any other Exception showing up here indicates a very fundamental bug in a
    // Future implementation.
    throw e.nonNullCause()
  }

  return suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
    addListener(ToContinuation(this, cont), MoreExecutors.directExecutor())
    cont.invokeOnCancellation { cancel(false) }
  }
}

/**
 * Propagates the outcome of [futureToObserve] to [continuation] on completion.
 *
 * Cancellation is propagated as cancelling the continuation. If [futureToObserve] completes and
 * fails, the cause of the Future will be propagated without a wrapping [ExecutionException] when
 * thrown.
 */
private class ToContinuation<T>(
  val futureToObserve: ApiFuture<T>,
  val continuation: CancellableContinuation<T>
) : Runnable {
  override fun run() {
    if (futureToObserve.isCancelled) {
      continuation.cancel()
    } else {
      try {
        continuation.resume(Uninterruptibles.getUninterruptibly(futureToObserve))
      } catch (e: ExecutionException) {
        // ExecutionException is the only kind of exception that can be thrown from a gotten
        // Future. Anything else showing up here indicates a very fundamental bug in a
        // Future implementation.
        continuation.resumeWithException(e.nonNullCause())
      }
    }
  }
}

/**
 * Returns the cause from an [ExecutionException] thrown by a [Future.get] or similar.
 *
 * [ExecutionException] _always_ wraps a non-null cause when Future.get() throws. A Future cannot
 * fail without a non-null `cause`, because the only way a Future _can_ fail is an uncaught
 * [Exception].
 *
 * If this !! throws [NullPointerException], a Future is breaking its interface contract and losing
 * state - a serious fundamental bug.
 */
private fun ExecutionException.nonNullCause(): Throwable {
  return this.cause!!
}

@InternalCoroutinesApi
private class ListenableFutureCoroutine<T>(context: CoroutineContext) :
  AbstractCoroutine<T>(context, initParentJob = true, active = true) {

  // JobListenableFuture propagates external cancellation to `this` coroutine. See
  // JobListenableFuture.
  @JvmField val future = JobListenableFuture<T>(this)

  override fun onCompleted(value: T) {
    future.complete(value)
  }

  override fun onCancelled(cause: Throwable, handled: Boolean) {
    // Note: if future was cancelled in a race with a cancellation of this
    // coroutine, and the future was successfully cancelled first, the cause of coroutine
    // cancellation is dropped in this promise. A Future can only be completed once.
    //
    // This is consistent with FutureTask behaviour. A race between a Future.cancel() and
    // a FutureTask.setException() for the same Future will similarly drop the
    // cause of a failure-after-cancellation.
    future.completeExceptionallyOrCancel(cause)
  }
}

/**
 * A [ListenableFuture] that delegates to an internal [SettableFuture], collaborating with it.
 *
 * This setup allows the returned [ListenableFuture] to maintain the following properties:
 * - Correct implementation of [Future]'s happens-after semantics documented for [get], [isDone] and
 *   [isCancelled] methods
 * - Cancellation propagation both to and from [Deferred]
 * - Correct cancellation and completion semantics even when this [ListenableFuture] is combined
 *   with different concrete implementations of [ListenableFuture]
 *     - Fully correct cancellation and listener happens-after obeying [Future] and
 *       [ListenableFuture]'s documented and implicit contracts is surprisingly difficult to
 *       achieve. The best way to be correct, especially given the fun corner cases from
 *       [AbstractFuture.setFuture], is to just use an [AbstractFuture].
 *     - To maintain sanity, this class implements [ListenableFuture] and uses an auxiliary
 *       [SettableFuture] around coroutine's result as a state engine to establish
 *       happens-after-completion. This could probably be compressed into one subclass of
 *       [AbstractFuture] to save an allocation, at the cost of the implementation's readability.
 */
private class JobListenableFuture<T>(private val jobToCancel: Job) : ListenableFuture<T> {
  /**
   * Serves as a state machine for [Future] cancellation.
   *
   * [AbstractFuture] has a highly-correct atomic implementation of `Future`'s completion and
   * cancellation semantics. By using that type, the [JobListenableFuture] can delegate its
   * semantics to `auxFuture.get()` the result in such a way that the `Deferred` is always complete
   * when returned.
   *
   * To preserve Coroutine's [CancellationException], this future points to either `T` or
   * [Cancelled].
   */
  private val auxFuture = SettableFuture.create<Any?>()

  /**
   * `true` if [auxFuture.get][ListenableFuture.get] throws [ExecutionException].
   *
   * Note: this is eventually consistent with the state of [auxFuture].
   *
   * Unfortunately, there's no API to figure out if [ListenableFuture] throws [ExecutionException]
   * apart from calling [ListenableFuture.get] on it. To avoid unnecessary [ExecutionException]
   * allocation we use this field as an optimization.
   */
  private var auxFutureIsFailed: Boolean = false

  /**
   * When the attached coroutine [isCompleted][Job.isCompleted] successfully its outcome should be
   * passed to this method.
   *
   * This should succeed barring a race with external cancellation.
   */
  fun complete(result: T): Boolean = auxFuture.set(result)

  /**
   * When the attached coroutine [isCompleted][Job.isCompleted] [exceptionally][Job.isCancelled] its
   * outcome should be passed to this method.
   *
   * This method will map coroutine's exception into corresponding Future's exception.
   *
   * This should succeed barring a race with external cancellation.
   */
  // CancellationException is wrapped into `Cancelled` to preserve original cause and message.
  // All the other exceptions are delegated to SettableFuture.setException.
  fun completeExceptionallyOrCancel(t: Throwable): Boolean =
    if (t is CancellationException) auxFuture.set(Cancelled(t))
    else auxFuture.setException(t).also { if (it) auxFutureIsFailed = true }

  /**
   * Returns cancellation _in the sense of [Future]_. This is _not_ equivalent to [Job.isCancelled].
   *
   * When done, this Future is cancelled if its [auxFuture] is cancelled, or if [auxFuture] contains
   * [CancellationException].
   *
   * See [cancel].
   */
  override fun isCancelled(): Boolean {
    // This expression ensures that isCancelled() will *never* return true when isDone() returns
    // false.
    // In the case that the deferred has completed with cancellation, completing `this`, its
    // reaching the "cancelled" state with a cause of CancellationException is treated as the
    // same thing as auxFuture getting cancelled. If the Job is in the "cancelling" state and
    // this Future hasn't itself been successfully cancelled, the Future will return
    // isCancelled() == false. This is the only discovered way to reconcile the two different
    // cancellation contracts.
    return auxFuture.isCancelled ||
      isDone &&
        !auxFutureIsFailed &&
        try {
          Uninterruptibles.getUninterruptibly(auxFuture) is Cancelled
        } catch (e: CancellationException) {
          // `auxFuture` got cancelled right after `auxFuture.isCancelled` returned false.
          true
        } catch (e: ExecutionException) {
          // `auxFutureIsFailed` hasn't been updated yet.
          auxFutureIsFailed = true
          false
        }
  }

  /**
   * Waits for [auxFuture] to complete by blocking, then uses its `result` to get the `T` value
   * `this` [ListenableFuture] is pointing to or throw a [CancellationException]. This establishes
   * happens-after ordering for completion of the entangled coroutine.
   *
   * [SettableFuture.get] can only throw [CancellationException] if it was cancelled externally.
   * Otherwise, it returns [Cancelled] that encapsulates outcome of the entangled coroutine.
   *
   * [auxFuture] _must be complete_ in order for the [isDone] and [isCancelled] happens-after
   * contract of [Future] to be correctly followed.
   */
  override fun get(): T {
    return getInternal(auxFuture.get())
  }

  /** See [get()]. */
  override fun get(timeout: Long, unit: TimeUnit): T {
    return getInternal(auxFuture.get(timeout, unit))
  }

  /** See [get()]. */
  private fun getInternal(result: Any?): T =
    if (result is Cancelled) {
      throw CancellationException().initCause(result.exception)
    } else {
      // We know that `auxFuture` can contain either `T` or `Cancelled`.
      @Suppress("UNCHECKED_CAST")
      result as T
    }

  override fun addListener(listener: Runnable, executor: Executor) {
    auxFuture.addListener(listener, executor)
  }

  override fun isDone(): Boolean {
    return auxFuture.isDone
  }

  /**
   * Tries to cancel [jobToCancel] if `this` future was cancelled. This is fundamentally racy.
   *
   * The call to `cancel()` will try to cancel [auxFuture]: if and only if cancellation of
   * [auxFuture] succeeds, [jobToCancel] will have its [Job.cancel] called.
   *
   * This arrangement means that [jobToCancel] _might not successfully cancel_, if the race resolves
   * in a particular way. [jobToCancel] may also be in its "cancelling" state while this
   * ListenableFuture is complete and cancelled.
   */
  override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
    // TODO: call jobToCancel.cancel() _before_ running the listeners.
    //  `auxFuture.cancel()` will execute auxFuture's listeners. This delays cancellation of
    //  `jobToCancel` until after auxFuture's listeners have already run.
    //  Consider moving `jobToCancel.cancel()` into [AbstractFuture.afterDone] when the API is
    // finalized.
    return if (auxFuture.cancel(mayInterruptIfRunning)) {
      jobToCancel.cancel()
      true
    } else {
      false
    }
  }

  override fun toString(): String = buildString {
    append(super.toString())
    append("[status=")
    if (isDone) {
      try {
        when (val result = Uninterruptibles.getUninterruptibly(auxFuture)) {
          is Cancelled -> append("CANCELLED, cause=[${result.exception}]")
          else -> append("SUCCESS, result=[$result]")
        }
      } catch (e: CancellationException) {
        // `this` future was cancelled by `Future.cancel`. In this case there's no cause or message.
        append("CANCELLED")
      } catch (e: ExecutionException) {
        append("FAILURE, cause=[${e.cause}]")
      } catch (t: Throwable) {
        // Violation of Future's contract, should never happen.
        append("UNKNOWN, cause=[${t.javaClass} thrown from get()]")
      }
    } else {
      append("PENDING, delegate=[$auxFuture]")
    }
    append(']')
  }
}

/**
 * A wrapper for `Coroutine`'s [CancellationException].
 *
 * If the coroutine is _cancelled normally_, we want to show the reason of cancellation to the user.
 * Unfortunately, [SettableFuture] can't store the reason of cancellation. To mitigate this, we wrap
 * cancellation exception into this class and pass it into [SettableFuture.complete]. See
 * implementation of [JobListenableFuture].
 */
private class Cancelled(@JvmField val exception: CancellationException)
