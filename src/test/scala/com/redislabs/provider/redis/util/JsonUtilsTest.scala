package com.redislabs.provider.redis.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * @author The Viet Nguyen
 */
class JsonUtilsTest extends AnyFunSuite with Matchers {

  test("testToJson") {
    val json = JsonUtils.toJson(Map("key" -> "value"))
    json shouldBe """{"key":"value"}"""
  }
}