package com.redislabs.provider.redis

import java.net.URI

import org.apache.spark.SparkConf
import redis.clients.jedis.util.{JedisClusterCRC16, JedisURIHelper, SafeEncoder}
import redis.clients.jedis.{Jedis, Protocol}

import scala.jdk.CollectionConverters._


/**
 * RedisEndpoint represents a redis connection endpoint info: host, port, auth password
 * db number, timeout and ssl mode
 *
 * @param host  the redis host or ip
 * @param port  the redis port
 * @param user  the authentication username
 * @param auth  the authentication password
 * @param dbNum database number (should be avoided in general)
 * @param ssl true to enable SSL connection. Defaults to false
 */
case class RedisEndpoint(host: String = Protocol.DEFAULT_HOST,
                         port: Int = Protocol.DEFAULT_PORT,
                         user: String = null,
                         auth: String = null,
                         dbNum: Int = Protocol.DEFAULT_DATABASE,
                         timeout: Int = Protocol.DEFAULT_TIMEOUT,
                         ssl: Boolean = false)
  extends Serializable {

  def this(conf: SparkConf) {
    this(
      conf.get("spark.redis.host", Protocol.DEFAULT_HOST),
      conf.getInt("spark.redis.port", Protocol.DEFAULT_PORT),
      conf.get("spark.redis.user", null),
      conf.get("spark.redis.auth", null),
      conf.getInt("spark.redis.db", Protocol.DEFAULT_DATABASE),
      conf.getInt("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT),
      conf.getBoolean("spark.redis.ssl", false)
    )
  }

  def this(conf: SparkConf, parameters: Map[String, String]) {
    this(
      parameters.getOrElse("host", conf.get("spark.redis.host", Protocol.DEFAULT_HOST)),
      parameters.getOrElse("port", conf.get("spark.redis.port", Protocol.DEFAULT_PORT.toString)).toInt,
      parameters.getOrElse("user", conf.get("spark.redis.user", null)),
      parameters.getOrElse("auth", conf.get("spark.redis.auth", null)),
      parameters.getOrElse("dbNum", conf.get("spark.redis.db", Protocol.DEFAULT_DATABASE.toString)).toInt,
      parameters.getOrElse("timeout", conf.get("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT.toString)).toInt,
      parameters.getOrElse("ssl", conf.get("spark.redis.ssl", false.toString)).toBoolean)
  }

  def this(uri: URI) {
    this(uri.getHost, uri.getPort, JedisURIHelper.getUser(uri), JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri),
      Protocol.DEFAULT_TIMEOUT, uri.getScheme == RedisSslScheme)
  }

  def this(uri: String) {
    this(URI.create(uri))
  }

  def connect(): Jedis = {
    ConnectionPool.connect(this)
  }

  def maskPassword(): RedisEndpoint = {
    this.copy(auth = "")
  }
}

case class RedisNode(endpoint: RedisEndpoint,
                     startSlot: Int,
                     endSlot: Int,
                     idx: Int,
                     total: Int) {
  def connect(): Jedis = {
    endpoint.connect()
  }
}

case class ReadWriteConfig(scanCount: Int, maxPipelineSize: Int, rddWriteIteratorGroupingSize: Int)

object ReadWriteConfig {
  val MaxPipelineSizeConfKey = "spark.redis.max.pipeline.size"
  val MaxPipelineSizeDefault = 100

  val ScanCountConfKey = "spark.redis.scan.count"
  val ScanCountDefault = 100

  val RddWriteIteratorGroupingSizeKey = "spark.redis.rdd.write.iterator.grouping.size"
  val RddWriteIteratorGroupingSizeDefault = 1000

  val Default: ReadWriteConfig = ReadWriteConfig(ScanCountDefault, MaxPipelineSizeDefault, RddWriteIteratorGroupingSizeDefault)

  def fromSparkConf(conf: SparkConf): ReadWriteConfig = {
    ReadWriteConfig(
      conf.getInt(ScanCountConfKey, ScanCountDefault),
      conf.getInt(MaxPipelineSizeConfKey, MaxPipelineSizeDefault),
      conf.getInt(RddWriteIteratorGroupingSizeKey, RddWriteIteratorGroupingSizeDefault)
    )
  }
}

object RedisConfig {
  def fromSparkConf(conf: SparkConf): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf))
  }

  def fromSparkConfAndParameters(conf: SparkConf, parameters: Map[String, String]): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf, parameters))
  }
}

class RedisConfig(val initialHost: RedisEndpoint) extends Serializable {

  val initialAddr: String = initialHost.host

  val hosts: Array[RedisNode] = getHosts(initialHost)
  val nodes: Array[RedisNode] = getNodes(initialHost)

  def getAuth: String = {
    initialHost.auth
  }

  def getDB: Int = {
    initialHost.dbNum
  }

  def getRandomNode(): RedisNode = {
    val rnd = scala.util.Random.nextInt().abs % hosts.length
    hosts(rnd)
  }

  def getNodesBySlots(sPos: Int, ePos: Int): Array[RedisNode] = {
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1

    nodes.filter(node => inter(sPos, ePos, node.startSlot, node.endSlot)).
      filter(_.idx == 0) //master only now
  }

  def connectionForKey(key: String): Jedis = {
    getHost(key).connect()
  }

  def connectionForKey(key: Array[Byte]): Jedis = {
    getHost(key).connect()
  }

  private def clusterEnabled(initialHost: RedisEndpoint): Boolean = {
    val conn = initialHost.connect()
    val info = conn.info.split("\n")
    val version = info.filter(_.contains("redis_version:"))(0)
    val clusterEnable = info.filter(_.contains("cluster_enabled:"))
    val mainVersion = version.substring(14, version.indexOf(".")).toInt
    val res = mainVersion > 2 && clusterEnable.length > 0 && clusterEnable(0).contains("1")
    conn.close()
    res
  }

  def getHost(key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    getHostBySlot(slot)
  }

  def getHost(key: Array[Byte]): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    getHostBySlot(slot)
  }

  private def getHostBySlot(slot: Int): RedisNode = {
    hosts.filter { host =>
      host.startSlot <= slot && host.endSlot >= slot
    }(0)
  }

  private def getHosts(initialHost: RedisEndpoint): Array[RedisNode] = {
    getNodes(initialHost).filter(_.idx == 0)
  }

  private def getNonClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val master = (initialHost.host, initialHost.port)
    val conn = initialHost.connect()

    val replinfo = conn.info("Replication").split("\n")
    conn.close()

    if (replinfo.exists(_.contains("role:slave"))) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt

      getNonClusterNodes(initialHost = RedisEndpoint(
        host = host,
        port = port,
        user = initialHost.user,
        auth = initialHost.auth,
        dbNum = initialHost.dbNum,
        ssl = initialHost.ssl
      ))

    } else {
      val slaves = replinfo.filter(x => x.contains("slave") && x.contains("online")).map(rl => {
        val content = rl.substring(rl.indexOf(':') + 1).split(",")
        val ip = content(0)
        val port = content(1)
        (ip.substring(ip.indexOf('=') + 1), port.substring(port.indexOf('=') + 1).toInt)
      })

      val nodes = master +: slaves
      val range = nodes.length
      (0 until range).map(i => {
        val endpoint = RedisEndpoint(
          host = nodes(i)._1,
          port = nodes(i)._2,
          user = initialHost.user,
          auth = initialHost.auth,
          dbNum = initialHost.dbNum,
          timeout = initialHost.timeout,
          ssl = initialHost.ssl)
        RedisNode(endpoint, 0, 16383, i, range)
      }).toArray
    }
  }

  private def getClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val conn = initialHost.connect()
    val res = conn.clusterSlots().asScala.flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
        val sPos = slotInfo.get(0).toString.toInt
        val ePos = slotInfo.get(1).toString.toInt
        (0 until (slotInfo.size - 2)).map(i => {
          val node = slotInfo.get(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
          val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
          val port = node.get(1).toString.toInt
          val endpoint = RedisEndpoint(
            host = host,
            port = port,
            user = initialHost.user,
            auth = initialHost.auth,
            dbNum = initialHost.dbNum,
            timeout = initialHost.timeout,
            ssl = initialHost.ssl)
          RedisNode(endpoint, sPos, ePos, i, slotInfo.size - 2)
        })
      }
    }.toArray
    conn.close()
    res
  }

  def getNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    if (clusterEnabled(initialHost)) {
      getClusterNodes(initialHost)
    } else {
      getNonClusterNodes(initialHost)
    }
  }
}