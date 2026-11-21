package com.github.olehbohatyi.mep.config

final case class HttpConfig(host: String, port: Int, rateLimit: RateLimitConfig, auth: AuthConfig)

/** At most `requests` event submissions per client in each window of `windowSeconds`; `requests = 0` turns the limit
  * off (the load test does, since all its traffic comes from one address).
  */
final case class RateLimitConfig(requests: Int, windowSeconds: Int)

/** Writes (event submissions and resolve requests) need `Authorization: Bearer <key>` with one of `keys`: a
  * comma-separated list of `name:sha256-hex` pairs, so the gate holds hashes and never the keys themselves
  * (`scripts/api-key.sh` makes a pair). `required = false` lets writes through without a key, for local experiments.
  */
final case class AuthConfig(required: Boolean, keys: String)
