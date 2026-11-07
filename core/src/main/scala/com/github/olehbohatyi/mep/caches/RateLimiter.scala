package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.*

import com.github.olehbohatyi.mep.config.RateLimitConfig

/** Counts requests per client in fixed windows. */
trait RateLimiter:
  def acquire(client: String): Task[RateLimiter.Decision]

object RateLimiter:

  enum Decision:
    case Allowed
    // The client may try again once the window ends, in `retryAfter` (whole seconds, at least one)
    case Limited(retryAfter: Duration)

  // Separate from the gate's idempotency keys (mep:gate:idem) and the pipe's processed ids
  val Namespace = "mep:gate:rate"

  val unlimited: RateLimiter = (_: String) => ZIO.succeed(Decision.Allowed)

  def apply(redis: Redis, config: RateLimitConfig, namespace: String = Namespace): RateLimiter =
    if config.requests <= 0 then unlimited else RateLimiterLive(redis, config, namespace)

  // A window of zero or less would make the script delete each counter at once: no limit, and no sign of it
  def validate(config: RateLimitConfig): Either[String, RateLimitConfig] =
    if config.requests > 0 && config.windowSeconds <= 0 then
      Left(s"rate-limit window-seconds must be positive, got ${config.windowSeconds}")
    else Right(config)

  val live: ZLayer[Redis & RateLimitConfig, IllegalArgumentException, RateLimiter] =
    ZLayer.fromZIO(
      for
        redis  <- ZIO.service[Redis]
        config <-
          ZIO.serviceWithZIO[RateLimitConfig](c => ZIO.fromEither(validate(c)).mapError(IllegalArgumentException(_)))
      yield apply(redis, config)
    )

  /** Counts and starts the window's expiry in one step: with INCR and EXPIRE sent separately, a gate stopped in between
    * would leave a counter that never expires and a client blocked for good. Returns the count and the milliseconds
    * left in the window.
    */
  private val Script =
    """local n = redis.call('INCR', KEYS[1])
      |local ttl = redis.call('PTTL', KEYS[1])
      |if ttl < 0 then
      |  redis.call('PEXPIRE', KEYS[1], ARGV[1])
      |  ttl = tonumber(ARGV[1])
      |end
      |return {n, ttl}""".stripMargin

  final private class RateLimiterLive(redis: Redis, config: RateLimitConfig, namespace: String) extends RateLimiter:

    private val windowMillis = config.windowSeconds.toLong * 1000

    def acquire(client: String): Task[Decision] =
      redis
        .eval(Script, Chunk.single(s"$namespace:$client"), Chunk.single(windowMillis.toString))(
          using Input.StringInput,
          Input.StringInput,
        )
        .returning[Chunk[Long]](using Output.ChunkOutput(Output.LongOutput))
        .flatMap {
          case Chunk(count, ttlMillis) =>
            ZIO.succeed(
              if count <= config.requests then Decision.Allowed
              else Decision.Limited(math.max(1L, (ttlMillis + 999) / 1000).seconds)
            )
          case other                   => ZIO.fail(IllegalStateException(s"unexpected rate limit reply: $other"))
        }
