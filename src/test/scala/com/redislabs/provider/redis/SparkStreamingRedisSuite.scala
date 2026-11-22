package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * For spark streaming test we have to create spark and streaming context for each test
 */
trait SparkStreamingRedisSuite extends AnyFunSuite with Env with BeforeAndAfterEach with Logging {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext
    ssc = new StreamingContext(sc, Seconds(1))
  }

  override protected def afterEach(): Unit = {
    ssc.stop()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    System.clearProperty("spark.driver.port")
    super.afterEach()
  }

}