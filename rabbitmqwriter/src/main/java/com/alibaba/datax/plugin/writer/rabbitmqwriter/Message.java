package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

public class Message {

    public static class FieldsInfo{
        private String name;
        private String property;
        private String value;

        public FieldsInfo(String name, String property, String value) {
            this.name = name;
            this.property = property;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String type) {
            this.property = type;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    private String type;
    private String dbtype;
    private String instance;
    private String table;
    private List<FieldsInfo> fieldsInfo = new ArrayList<FieldsInfo>();

    public Message(String type, String dbtype, String instance, String table) {
        this.type = type;
        this.dbtype = dbtype;
        this.instance = instance;
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDbtype() {
        return dbtype;
    }

    public void setDbtype(String dbtype) {
        this.dbtype = dbtype;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<FieldsInfo> getFieldsInfo() {
        return fieldsInfo;
    }

    public void setFieldsInfo(FieldsInfo fieldsInfo) {
        this.fieldsInfo.add(fieldsInfo);
    }

    public String toString(){
        return JSON.toJSONString(this);
    }

    public static void main(String[] args){
        FieldsInfo fieldsInfo = new FieldsInfo("id1","string", "1111");
        FieldsInfo fieldsInfo1 = new FieldsInfo("id1","string", "1111");
        Message message = new Message("1", "1", "dbtest", "tbl_test");
        message.setFieldsInfo(fieldsInfo);
        message.setFieldsInfo(fieldsInfo1);
        String str = message.toString();
        System.out.println(str);
    }
}
