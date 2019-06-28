package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.util.Configuration;
import redis.clients.jedis.Jedis;

import java.util.*;

public class KeySetSplitUtil {

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber, Jedis jedisClient){
        List<Configuration> confList = new ArrayList<Configuration>();

        List<List<String>> keysList = doSplitKeySet(jedisClient, adviceNumber);

        for(int i=0; i<keysList.size(); i++){
            Configuration sliceConfig = originalSliceConfig.clone();
            sliceConfig.set(Constants.KEYS, keysList.get(i));
            confList.add(sliceConfig);
        }
        return confList;
    }

    private static List<List<String>> doSplitKeySet(Jedis jedisclient, int adviceNUmber){
        List<List<String>> keySetList = new ArrayList<List<String>>();

        Set<String> keys = jedisclient.keys("*");
        int eachSplitSetNumber = keys.size() / adviceNUmber;
        if(keys.size() % adviceNUmber != 0){
            eachSplitSetNumber ++;
        }
        Iterator<String> ite = keys.iterator();
        int n = 0;
        List<String> tmpKeySet = new ArrayList<String>();
        while(ite.hasNext()){
            if(n > eachSplitSetNumber){
                keySetList.add(tmpKeySet);
                tmpKeySet = new ArrayList<String>();
                n = 0;
            }
            tmpKeySet.add(ite.next());
            n++;
        }
        if(n != 0){
            keySetList.add(tmpKeySet);
        }
        return keySetList;
    }
}
