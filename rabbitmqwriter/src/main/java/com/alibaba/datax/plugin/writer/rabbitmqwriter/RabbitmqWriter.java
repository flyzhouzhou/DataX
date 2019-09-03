package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.messages.JobStateStructure;
import com.alibaba.datax.common.messages.RabbitmqUtils;
import com.alibaba.datax.common.messages.RedisStorage;
import com.alibaba.datax.common.messages.ZeromqUtils;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.getProperty;

public class RabbitmqWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String fieldDelimiter;
        private boolean message;

        private long recordNumBeforSleep;
        private long sleepTime;

        private static Pattern jobIdPattern = Pattern.compile("(.+)-.+-.+-.+");
        private String type;
        private String dbType;
        private String dbName;
        private static String columnBlobType = "blob";
        private static String binaryString = "binarystring";
        private String baseDir;
        private RabbitmqUtils rabbitmqUtils;
        private ZeromqUtils zeromqUtils;


        @Override
        public void init() {

            String currentName = Thread.currentThread().getName();
            String jobId = parseJobIdFromThreadName(currentName);
            if(jobId == null){
            }else{
                RedisStorage redisStorage = new RedisStorage();
                JobStateStructure jobStateStructure = redisStorage.readJobStatus(Long.parseLong(jobId));
                this.type = jobStateStructure.getType();
                this.dbType = jobStateStructure.getDbType();
                this.dbName = jobStateStructure.getDbName();
            }
            if(this.type == null) { this.type = "no";}
            if(this.dbType == null) { this.dbType = "no";}
            if(this.dbName == null) { this.dbName = "no";}

            this.writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = this.writerSliceConfig.getString(
                    Key.FIELD_DELIMITER, "\t");
            this.message = this.writerSliceConfig.getBool(Key.MESSAGE, true);

            this.recordNumBeforSleep = this.writerSliceConfig.getLong(Key.RECORD_NUM_BEFORE_SLEEP, 0);
            this.sleepTime = this.writerSliceConfig.getLong(Key.SLEEP_TIME, 0);

            this.rabbitmqUtils = new RabbitmqUtils();
            this.zeromqUtils = new ZeromqUtils();
        }

        @Override
        public void prepare() {
            String envPath = System.getProperty("datax.home");
            this.baseDir = envPath + "\\filebak\\";
            File dir = new File(this.baseDir);
            try {
                if (!dir.exists()) {
                    dir.mkdirs();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {

                    getTaskPluginCollector().collectTableState(record.getCurrentTable(), System.currentTimeMillis());

                    if (this.message) {
                        String msg = recordToString(record);
                        rabbitmqUtils.produceMessage(msg);
                        //zeromqUtils.produceMessage(msg);
                    } else {
                        /* do nothing */
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return "";
            }
            //String dbInstance = record.getDbInstance();
            String table = record.getCurrentTable();
            Message message = new Message(this.type,this.dbType, this.dbName, table);
            Column column;
            for (int i = 0; i < recordLength; i++) {
                String columnName = record.getColumnNames(i);
                String columnType = record.getColumnTypes(i);
                column = record.getColumn(i);
                String columnValue;
                if(columnType.equals(columnBlobType) || columnType.equals(binaryString)){
                    columnValue = saveBlobDataToFile(column.asBytes());
                }else{
                    columnValue = column.asString();
                }
                Message.FieldsInfo fieldsInfo = new Message.FieldsInfo(columnName, columnType, columnValue);
                message.setFieldsInfo(fieldsInfo);
            }
            return message.toString();
        }

        private static String parseJobIdFromThreadName(String threadName) {
            Matcher jobId = jobIdPattern.matcher(threadName);
            if (jobId.matches()) {
                return jobId.group(1);
            }
            return null;
        }

        private String saveBlobDataToFile(byte[] rawData){
            String[] filenameList = new File(this.baseDir).list();
            String fullFileName = "";
            try{
                String filename = UUID.randomUUID().toString().replace('-', '_');
                while (isHave(filenameList, filename)) {
                    filename = UUID.randomUUID().toString().replace('-', '_');
                }
                fullFileName = this.baseDir + filename;
                FileOutputStream fos = new FileOutputStream(fullFileName);
                fos.write(rawData);
                fos.close();
            }catch(Exception e){
                e.printStackTrace();
            }
            return fullFileName;
        }

        private static boolean isHave(String[] strs,String s){
            for(int i=0;i<strs.length;i++){
                if(strs[i].indexOf(s) != -1){
                    return true;
                }
            }
            return false;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
