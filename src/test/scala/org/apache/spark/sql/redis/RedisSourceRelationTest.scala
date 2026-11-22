package org.apache.spark.sql.redis

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * @author The Viet Nguyen
 */
class RedisSourceRelationTest extends AnyFunSuite with Matchers {

  test("redis key extractor with prefix pattern") {
    val key = RedisSourceRelation.tableKey("table*", "tablekey")
    key shouldBe "key"
  }

  test("redis key extractor with other patterns") {
    val key = RedisSourceRelation.tableKey("*table", "key")
    key shouldBe "key"
  }
}