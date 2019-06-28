package com.alibaba.datax.common.messages;


import redis.clients.jedis.Jedis;


public class RedisStorage {

    private Jedis jedisClient;

    private String host = "127.0.0.1";

    private int port = 6379;

    private String password = "123";

    public RedisStorage(){
        RedisUtils redisUtils = new RedisUtils(host, port, password);
        jedisClient = redisUtils.getJedisClient();
    }

    public void writeJobStatus(long jobId, boolean status){
        RedisUtils.hset(this.jedisClient, String.valueOf(jobId), Constants.STATUS, String.valueOf(status));
    }
}
