package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.rdd.Keys
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession, SQLImplicits => ClassicSQLImplicits}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author The Viet Nguyen
 */
trait SparkRedisSuite extends AnyFunSuite with Env with Keys with BeforeAndAfterAll {

  override def beforeAll() {
    super.beforeAll()
    spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    System.clearProperty("spark.driver.port")
  }

  object TestSqlImplicits extends ClassicSQLImplicits {

    override protected def session: ClassicSparkSession = spark.asInstanceOf[ClassicSparkSession]
  }

}