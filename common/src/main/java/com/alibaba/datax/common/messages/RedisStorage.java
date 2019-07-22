package com.alibaba.datax.common.messages;


import redis.clients.jedis.Jedis;


public class RedisStorage {

    private Jedis jedisClient;

    private String host = "127.0.0.1";

    private int port = 6379;

    //private String password = "123";

    public RedisStorage(){
        RedisUtils redisUtils = new RedisUtils(host, port);
        jedisClient = redisUtils.getJedisClient();
    }

    public void writeJobStatus(long jobId, JobStateStructure state){
        RedisUtils.set(this.jedisClient, String.valueOf(jobId), state.toString());
    }

    public JobStateStructure readJobStatus(long jobId){
        Object object = RedisUtils.get(this.jedisClient, String.valueOf(jobId));
        return JobStateStructure.parseJsonString((String)object);
    }

    public static void main(String[] args){
        RedisStorage redisStorage = new RedisStorage();
        //redisStorage.writeJobStatus(100,false);
    }
}
