package com.redislabs.provider.redis

import com.redislabs.provider.redis.util.Logging
import org.apache.commons.pool2.PooledObject
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}
import redis.clients.jedis.{DefaultJedisClientConfig, HostAndPort, Jedis, JedisClientConfig, JedisFactory, JedisPool,
  JedisPoolConfig, Protocol}

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap


object ConnectionPool extends Logging {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  /**
   * Custom JedisFactory that sends CLIENT SETINFO commands when a new connection is created.
   * This ensures CLIENT SETINFO is sent only once per connection (at creation time),
   * not every time a connection is borrowed from the pool.
   */
  private class SparkRedisJedisFactory(hostAndPort: HostAndPort, clientConfig: JedisClientConfig)
    extends JedisFactory(hostAndPort, clientConfig) {

    override def makeObject(): PooledObject[Jedis] = {
      val pooledObject = super.makeObject()
      try {
        val jedis = pooledObject.getObject
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-NAME", RedisClientLibraryInfo.libName)
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-VER", RedisClientLibraryInfo.libVersion)
      } catch {
        case e: JedisDataException =>
          logDebug(s"CLIENT SETINFO not supported (requires Redis 7.2+): ${e.getMessage}")
      }
      pooledObject
    }
  }

  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.computeIfAbsent(re, _ => {
      val poolConfig: JedisPoolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(250)
      poolConfig.setMaxIdle(32)
      poolConfig.setTestOnBorrow(false)
      poolConfig.setTestOnReturn(false)
      poolConfig.setTestWhileIdle(false)
      poolConfig.setSoftMinEvictableIdleTime(Duration.ofMinutes(1))
      poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30))
      poolConfig.setNumTestsPerEvictionRun(-1)

      val clientConfig: JedisClientConfig = DefaultJedisClientConfig.builder()
        .connectionTimeoutMillis(re.timeout)
        .socketTimeoutMillis(re.timeout)
        .user(re.user)
        .password(re.auth)
        .database(re.dbNum)
        .ssl(re.ssl)
        .build()

      val factory = new SparkRedisJedisFactory(new HostAndPort(re.host, re.port), clientConfig)
      new JedisPool(poolConfig, factory)
    })

    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      }
      catch {
        case e: JedisConnectionException if Option(e.getCause).exists(_.toString.
          contains("ERR max number of clients reached")) => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}