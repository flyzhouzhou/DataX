package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.messages.RedisUtils;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.base.Strings;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisReader extends Reader {
    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private Jedis jedisClient;

        private String host;

        private int port;

        private String password;

        private int dbInstance;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return KeySetSplitUtil.doSplit(originalConfig, adviceNumber, jedisClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.host = originalConfig.getString(Constants.HOST);
            this.port = originalConfig.getInt(Constants.PORT, 6379);
            this.password = originalConfig.getString(Constants.PASSWORD);
            this.dbInstance = originalConfig.getInt(Constants.DBINSTANCE, 0);
            RedisUtils redisUtils = null;
            if(!Strings.isNullOrEmpty(password)){
                redisUtils = new RedisUtils(this.host, this.port, this.password);
            }else{
                redisUtils = new RedisUtils(this.host, this.port);
            }

            this.jedisClient = redisUtils.getJedisClient();
            jedisClient.select(dbInstance);
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private Jedis jedisClient;

        private String host;

        private int port;

        private String password;

        private int dbInstance;

        private List<String> keys;


        @Override
        public void startRead(RecordSender recordSender) {

            Iterator<String> ite = this.keys.iterator();
            while(ite.hasNext()){
                Record record = recordSender.createRecord();
                record.setDbInstance(String.valueOf(this.dbInstance));
                record.setCurrentTable("no-table");
                String key = ite.next();
                record.setColumnNames(key);
                String type = jedisClient.type(key);
                Object tempCol = null;
                if(type.equals("string")){
                    tempCol = RedisUtils.get(this.jedisClient, key);
                }else if(type.equals("hash")){
                    tempCol = RedisUtils.hgetAll(this.jedisClient, key);
                }else if(type.equals("list")){
                    tempCol = RedisUtils.lget(this.jedisClient, key);
                }else if(type.equals("set")){
                    tempCol = RedisUtils.sget(this.jedisClient, key);
                }

                if (tempCol == null) {
                    //continue; 这个不能直接continue会导致record到目的端错位
                    record.addColumn(new StringColumn(null));
                } else if (tempCol instanceof String) {
                    record.setColumnTypes("string");
                    String colString = StrUtils.makeStringToString((String)tempCol, key);
                    record.addColumn(new StringColumn(colString));
                } else if (tempCol instanceof List) {
                    record.setColumnTypes("list");
                    List<String> list = (List<String>) tempCol;
                    String colString = StrUtils.getListToString(list, key);
                    record.addColumn(new StringColumn(colString));
                } else if (tempCol instanceof Map) {
                    record.setColumnTypes("map");
                    Map<String, String> map = (Map<String, String>) tempCol;
                    String colString = StrUtils.getMapToString(map, key);
                    record.addColumn(new StringColumn(colString));
                } else if (tempCol instanceof Set) {
                    record.setColumnTypes("set");
                    Set<String> set = (Set<String>) tempCol;
                    String colString = StrUtils.getSetToString(set, key);
                    record.addColumn(new StringColumn(colString));
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.host = readerSliceConfig.getString(Constants.HOST);
            this.port = readerSliceConfig.getInt(Constants.PORT, 6379);
            this.password = readerSliceConfig.getString(Constants.PASSWORD);
            this.dbInstance = readerSliceConfig.getInt(Constants.DBINSTANCE, 0);
            this.keys = readerSliceConfig.getList(Constants.KEYS, String.class);
            RedisUtils redisUtils = null;
            if(!Strings.isNullOrEmpty(password)){
                redisUtils = new RedisUtils(this.host, this.port, this.password);
            }else{
                redisUtils = new RedisUtils(this.host, this.port);
            }

            this.jedisClient = redisUtils.getJedisClient();
            jedisClient.select(this.dbInstance);
        }

        @Override
        public void destroy() {

        }
    }
}
