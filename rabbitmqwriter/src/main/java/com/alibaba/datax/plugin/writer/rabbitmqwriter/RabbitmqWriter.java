package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.messages.RabbitmqUtils;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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


        @Override
        public void init() {
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
            String dbInstance = record.getDbInstance();
            String table = record.getCurrentTable();
            Message message = new Message("1","1", dbInstance, table);
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

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
