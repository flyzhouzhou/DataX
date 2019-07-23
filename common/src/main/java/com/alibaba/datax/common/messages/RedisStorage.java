package com.alibaba.datax.common.messages;


import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.util.Properties;


public class RedisStorage {

    private Jedis jedisClient;

    private String host;

    private int port;

    private String password;

    public RedisStorage(){
        String envPath = System.getProperty("datax.home");
        String properPath = envPath + "\\conf\\redisserver.properties";
        try{
            Properties properties = new Properties();
            FileInputStream fis = new FileInputStream(properPath);
            properties.load(fis);
            fis.close();
            host = properties.getProperty("redis.host");
            port = Integer.parseInt(properties.getProperty("redis.port"));
            password = properties.getProperty("redis.password");
        }catch(Exception e){
            e.printStackTrace();
        }

        if(password == null || password.equals("")){
            RedisUtils redisUtils = new RedisUtils(host, port);
            jedisClient = redisUtils.getJedisClient();
        }else{
            RedisUtils redisUtils = new RedisUtils(host, port, password);
            jedisClient = redisUtils.getJedisClient();
        }
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
