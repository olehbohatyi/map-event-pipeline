package com.github.olehbohatyi.mep.gate

import zio.*

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat

import com.github.olehbohatyi.mep.config.{ AuthConfig, HttpConfig }

/** Who may write. A request's key is hashed and looked up among the configured hashes: the gate never holds a key, and
  * a lookup by hash tells a timing attacker nothing about any key.
  */
final class ApiKeys private (clients: Map[String, String], val required: Boolean):

  /** The client name for an `Authorization` header value, `None` when keys are not required and none was sent, or why
    * the request is refused. A key that is sent is always checked, required or not.
    */
  def check(authorization: Option[String]): Either[String, Option[String]] =
    authorization.map(_.trim).filter(_.nonEmpty) match
      case None if required => Left("an API key is required: send Authorization: Bearer <key>")
      case None             => Right(None)
      case Some(header)     =>
        ApiKeys.bearer(header) match
          case None      => Left("send the API key as Authorization: Bearer <key>")
          case Some(key) => clients.get(ApiKeys.hash(key)).map(Some(_)).toRight("unknown API key")

object ApiKeys:

  private val Name = "[A-Za-z0-9_.-]{1,64}".r
  private val Hash = "[0-9a-f]{64}".r

  def hash(key: String): String =
    HexFormat.of.formatHex(MessageDigest.getInstance("SHA-256").digest(key.getBytes(UTF_8)))

  private def bearer(header: String): Option[String] =
    header.split("\\s+", 2) match
      case Array(scheme, token) if scheme.equalsIgnoreCase("Bearer") && token.trim.nonEmpty => Some(token.trim)
      case _                                                                                => None

  // Anyone may write (keys still identify a client that sends one); for tests and local experiments
  val open: ApiKeys = ApiKeys(Map.empty, required = false)

  def fromConfig(config: AuthConfig): Either[String, ApiKeys] =
    val entries = config.keys.split(",").map(_.trim).filter(_.nonEmpty).toList
    val parsed  = entries.map { entry =>
      entry.split(":", 2) match
        case Array(name, _) if !Name.matches(name)    =>
          Left(s"API key name '$name' must be 1-64 letters, digits, '_', '.' or '-'")
        case Array(name, hash) if !Hash.matches(hash) => Left(s"API key '$name' needs a SHA-256 hash in lowercase hex")
        case Array(name, hash)                        => Right(hash -> name)
        case _                                        => Left("API keys are name:sha256-hex pairs, comma-separated")
    }
    for
      pairs <- parsed.collectFirst { case Left(err) => err }.toLeft(parsed.collect { case Right(p) => p })
      _     <- pairs
                 .groupBy(_._2)
                 .collectFirst { case (n, ps) if ps.size > 1 => s"API key name '$n' is used twice" }
                 .toLeft(())
      _     <- pairs
                 .groupBy(_._1)
                 .collectFirst { case (_, ps) if ps.size > 1 => "the same key is configured twice" }
                 .toLeft(())
      _     <- Either.cond(
                 pairs.nonEmpty || !config.required,
                 (),
                 "no API keys configured (GATE_API_KEYS); set GATE_AUTH_REQUIRED=false to accept writes without one",
               )
    yield ApiKeys(pairs.toMap, config.required)

  val live: ZLayer[HttpConfig, IllegalArgumentException, ApiKeys] =
    ZLayer.fromZIO(
      ZIO.serviceWithZIO[HttpConfig](c => ZIO.fromEither(fromConfig(c.auth)).mapError(IllegalArgumentException(_)))
    )
