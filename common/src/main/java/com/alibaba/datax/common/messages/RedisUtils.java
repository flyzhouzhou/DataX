package com.alibaba.datax.common.messages;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
    private  JedisPool jedisPool;

    public RedisUtils(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(30);
        config.setMaxIdle(10);
        jedisPool = new JedisPool(config, host, port, 60);
    }

    public RedisUtils(String host, int port, String password) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(30);
        config.setMaxIdle(10);
        jedisPool = new JedisPool(config, host, port, 60, password);
    }

    public Jedis getJedisClient(){
        Jedis jedis = null;
        try{
            jedis = jedisPool.getResource();
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return jedis;
    }

    public static boolean hasKey(Jedis jedisClient, String key){
        try{
            return jedisClient.exists(key);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static boolean hset(Jedis jedisClient, String key, String item, String value){
        try{
            jedisClient.hset(key, item, value);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // map
    public static Object hgetAll(Jedis jedisClient, String key){
        Object result = null;
        try{
            result = jedisClient.hgetAll(key);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return result;
    }

    //string
    public static Object get(Jedis jedisClient, String key){
        Object result = null;
        try{
            result = jedisClient.get(key);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return result;
    }

    //list
    public static Object lget(Jedis jedisClient, String key){
        Object result = null;
        try{
            result = jedisClient.lrange(key, 0 , -1);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return result;
    }

    //set
    public static Object sget(Jedis jedisClient, String key){
        Object result = null;
        try{
            result = jedisClient.smembers(key);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return result;
    }
}
