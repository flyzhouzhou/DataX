package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.messages.JobStateStructure;
import com.alibaba.datax.common.messages.RabbitmqUtils;
import com.alibaba.datax.common.messages.RedisStorage;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {

                    getTaskPluginCollector().collectTableState(record.getCurrentTable(), System.currentTimeMillis());

                    if (this.message) {
                        String msg = recordToString(record);
                        RabbitmqUtils.produceMessage(msg);
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
                String columnValue = column.asString();
                Message.FieldsInfo fieldsInfo = new Message.FieldsInfo(columnName, columnType, columnValue);
                message.setFieldsInfo(fieldsInfo);
            }
            return message.toString();
        }

        public static String parseJobIdFromThreadName(String threadName) {
            Matcher jobId = jobIdPattern.matcher(threadName);
            if (jobId.matches()) {
                return jobId.group(1);
            }
            return null;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
