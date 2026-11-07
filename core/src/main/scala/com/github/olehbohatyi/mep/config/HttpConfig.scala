package com.github.olehbohatyi.mep.config

final case class HttpConfig(host: String, port: Int, rateLimit: RateLimitConfig)

/** At most `requests` event submissions per client in each window of `windowSeconds`; `requests = 0` turns the limit
  * off (the load test does, since all its traffic comes from one address).
  */
final case class RateLimitConfig(requests: Int, windowSeconds: Int)
