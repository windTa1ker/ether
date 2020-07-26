package com.windTa1ker.photon.utils.redis

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.annotation.meta.getter

/**
 * @time: 2020/7/2 12:41 下午
 * @author: 荆旗
 * @Description:
 */
object RedisClusterUtil {

  @getter
  private var jedisCluster: JedisCluster = _

  def connect(re: RedisEndpoint): JedisCluster = {
    if (null != jedisCluster) jedisCluster else {
      jedisCluster = create(re)
      jedisCluster
    }
  }

  private def create(re: RedisEndpoint): JedisCluster = {
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(1000)
    poolConfig.setMaxIdle(64)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    poolConfig.setMinEvictableIdleTimeMillis(180000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    new JedisCluster(new HostAndPort(re.host, re.port),re.timeout,re.timeout,5,re.auth, poolConfig)
  }

}
