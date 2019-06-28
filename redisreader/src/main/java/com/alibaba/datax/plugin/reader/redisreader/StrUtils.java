package com.alibaba.datax.plugin.reader.redisreader;

import java.util.*;

public class StrUtils {
    public static String getMapToString(Map<String,String> map, String objKey){
        Set<String> keySet = map.keySet();
        String[] keyArray = keySet.toArray(new String[keySet.size()]);
        Arrays.sort(keyArray);
        StringBuilder sb = new StringBuilder();
        sb.append(objKey).append(":").append("{");
        for (int i=0; i< keyArray.length; i++) {
            if(map.get(keyArray[i]).trim().length() > 0){
                sb.append(keyArray[i]).append("=").append(map.get(keyArray[i]).trim());
            }
            if(i != keyArray.length-1){
                sb.append("&");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public static String getListToString(List<String> list, String objKey){
        StringBuilder sb = new StringBuilder();
        sb.append(objKey).append(":").append("[");
        for(int i=0; i < list.size(); i++){
            sb.append(list.get(i));
            if(i != list.size()-1){
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static String getSetToString(Set<String> set, String objKey){
        StringBuilder sb = new StringBuilder();
        sb.append(objKey).append(":").append("(");
        Iterator<String> ite = set.iterator();
        while(ite.hasNext()){
            sb.append(ite.next()).append(",");
        }
        sb.append(")");
        return sb.toString();
    }

    public static String makeStringToString(String str, String objKey){
        StringBuilder sb = new StringBuilder();
        sb.append(objKey).append(":");
        sb.append(str);
        return sb.toString();
    }
}
