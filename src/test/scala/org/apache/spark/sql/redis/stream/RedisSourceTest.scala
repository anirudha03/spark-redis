package org.apache.spark.sql.redis.stream

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

/**
 * @author The Viet Nguyen
 */
class RedisSourceTest extends AnyFunSuite with Matchers with OptionValues {

  test("testGetOffsetRanges") {
    val startOffsets = RedisSourceOffset(Map("mystream" -> RedisConsumerOffset("group55", "0-0")))
    val endOffsets = RedisSourceOffset(Map("mystream" -> RedisConsumerOffset("group55", "0-1")))
    val consumerConfig = RedisConsumerConfig("mystream", "group55", "consumer", 1000, 100)
    val consumerConfigs = Seq(consumerConfig)
    val offsetRanges = RedisSource.getOffsetRanges(Some(startOffsets), endOffsets, consumerConfigs)
    offsetRanges.head shouldBe RedisSourceOffsetRange(Some("0-0"), "0-1", consumerConfig)
  }
}