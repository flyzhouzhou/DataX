package com.alibaba.datax.common.messages;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class JobStateStructure {

    private String type;

    private String dbType;

    private String dbName;

    private String percentage;

    private String state;

    private Set<String> finishedTables;

    private Set<String> nowTables;

    public JobStateStructure(){
        this.finishedTables = new HashSet<String>();
        this.nowTables = new HashSet<String>();
    }

    public JobStateStructure(String type, String dbType, String dbName, String percentage, String state) {
        this.type = type;
        this.dbType = dbType;
        this.dbName = dbName;
        this.percentage = percentage;
        this.state = state;
        this.finishedTables = new HashSet<String>();
        this.nowTables = new HashSet<String>();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getPercentage() {
        return percentage;
    }

    public void setPercentage(String percentage) {
        this.percentage = percentage;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Set<String> getFinishedTables() {
        return finishedTables;
    }

    public void setFinishedTables(Set<String> finishedTables) {
        this.finishedTables = finishedTables;
    }

    public Set<String> getNowTables() {
        return nowTables;
    }

    public void setNowTables(Set<String> nowTables) {
        this.nowTables = nowTables;
    }

    public String toString(){
        return JSON.toJSONString(this);
    }

    public static JobStateStructure parseJsonString(String jsonString){
        JSONObject object = JSONObject.parseObject(jsonString);
        return JSON.toJavaObject(object, JobStateStructure.class);
    }
}
