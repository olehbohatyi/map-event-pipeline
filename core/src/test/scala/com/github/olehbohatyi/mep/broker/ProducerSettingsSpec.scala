package com.github.olehbohatyi.mep.broker

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.testkit.TestKafka

object ProducerSettingsSpec extends ZIOSpecDefault:

  def spec = suite("ProducerService.settings")(
    // Regression: with Kafka's defaults a publish to a broker that stopped answering waited up to three minutes
    test("bounds a publish by the delivery timeout, with a request timeout Kafka accepts") {
      val props = ProducerService.settings(TestKafka.config().copy(deliveryTimeout = 10.seconds)).properties
      assertTrue(
        props("delivery.timeout.ms") == "10000",
        props("max.block.ms") == "10000",
        // Kafka refuses delivery.timeout.ms < linger.ms + request.timeout.ms
        props("request.timeout.ms").toString.toLong + props.get("linger.ms").fold(0L)(_.toString.toLong) <= 10000L,
      )
    },
    // With no linger every record went as its own request, and under the stress profile POST p95 rose to 390 ms
    test("holds records back 5 ms to batch them unless another linger is configured, within the delivery timeout") {
      val default                                       = ProducerService.settings(TestKafka.config()).properties
      val lingers                                       = ProducerService.settings(TestKafka.config().copy(linger = 2.seconds)).properties
      def long(props: Map[String, AnyRef], key: String) = props(key).toString.toLong
      assertTrue(
        long(default, "linger.ms") == 5L,
        long(lingers, "linger.ms") == 2000L,
        long(lingers, "linger.ms") + long(lingers, "request.timeout.ms") <= long(lingers, "delivery.timeout.ms"),
      )
    },
  )
