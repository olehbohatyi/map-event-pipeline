package com.github.olehbohatyi.mep.gate

import zio.test.*

import com.github.olehbohatyi.mep.config.AuthConfig

object ApiKeysSpec extends ZIOSpecDefault:

  private val aliceHash = ApiKeys.hash("alice-key")
  private val bobHash   = ApiKeys.hash("bob-key")

  private def keys(config: String, required: Boolean = true) = ApiKeys.fromConfig(AuthConfig(required, config))

  def spec = suite("ApiKeys")(
    test("hashes a key to lowercase SHA-256 hex") {
      // echo -n abc | shasum -a 256
      assertTrue(ApiKeys.hash("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
    },
    test("names the client of a known key, case-insensitive in the scheme, and refuses anything else") {
      val parsed = keys(s" alice:$aliceHash , bob:$bobHash ").toOption.get
      assertTrue(
        parsed.check(Some("Bearer alice-key")) == Right(Some("alice")),
        parsed.check(Some("bearer  bob-key ")) == Right(Some("bob")),
        parsed.check(Some("Bearer carol-key")) == Left("unknown API key"),
        // The hash itself is not a key
        parsed.check(Some(s"Bearer $aliceHash")) == Left("unknown API key"),
        parsed.check(Some("Basic YWxpY2U6a2V5")).isLeft,
        parsed.check(Some("Bearer")).isLeft,
        parsed.check(None).isLeft,
        parsed.check(Some("  ")).isLeft,
      )
    },
    test("without required keys lets an unkeyed request through, but still checks a key that is sent") {
      val parsed = keys(s"alice:$aliceHash", required = false).toOption.get
      assertTrue(
        parsed.check(None) == Right(None),
        parsed.check(Some("Bearer alice-key")) == Right(Some("alice")),
        parsed.check(Some("Bearer wrong")) == Left("unknown API key"),
        keys("", required = false).isRight,
      )
    },
    test("refuses a configuration it cannot trust, so the gate does not start with it") {
      assertTrue(
        keys("").left.exists(_.contains("GATE_AUTH_REQUIRED=false")),
        keys(s"alice$aliceHash").isLeft,
        keys("alice:not-a-hash").left.exists(_.contains("SHA-256")),
        keys(s"alice:${aliceHash.toUpperCase}").isLeft,
        keys(s"al ice:$aliceHash").isLeft,
        keys(s"alice:$aliceHash,alice:$bobHash").left.exists(_.contains("twice")),
        keys(s"alice:$aliceHash,bob:$aliceHash").left.exists(_.contains("twice")),
      )
    },
  )
