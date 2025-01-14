package com.alibaba.datax.plugin.reader.rdbmsreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.reader.ColumnType;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubCommonRdbmsReader extends CommonRdbmsReader {
    /*static {
        DBUtil.loadDriverClass("reader", "rdbms");
    }*/

    public static class Job extends CommonRdbmsReader.Job {
        static {
            DBUtil.loadDriverClass("reader", "rdbms");
        }

        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsReader.Task {
        static {
            DBUtil.loadDriverClass("reader", "rdbms");
        }

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        protected Record transportOneRecord(RecordSender recordSender,
                ResultSet rs, ResultSetMetaData metaData, int columnNumber,
                String mandatoryEncoding,
                TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();
            record.setDbInstance(this.dbInstance);
            record.setCurrentTable(this.currentTable);

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    record.setColumnNames(metaData.getColumnName(i));
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        record.setColumnTypes(ColumnType.Ty_STRING);
                        String rawData;
                        if (StringUtils.isBlank(mandatoryEncoding)) {
                            rawData = rs.getString(i);
                        } else {
                            rawData = new String(
                                    (rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY
                                            : rs.getBytes(i)),
                                    mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.setColumnTypes(ColumnType.Ty_BLOB);
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.setColumnTypes(ColumnType.Ty_INTERGER);
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.setColumnTypes(ColumnType.Ty_NUMERIC);
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.setColumnTypes(ColumnType.Ty_FLOAT);
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.setColumnTypes(ColumnType.Ty_TIME);
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        record.setColumnTypes(ColumnType.Ty_DATE);
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase(
                                "year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.setColumnTypes(ColumnType.Ty_TIMESTAMP);
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.setColumnTypes(ColumnType.Ty_BLOB);
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.setColumnTypes(ColumnType.Ty_BOOLEAN);
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if (rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;
                    //case Types.TIME_WITH_TIMEZONE:
                    //case Types.TIMESTAMP_WITH_TIMEZONE:
                    //    record.addColumn(new StringColumn(rs.getString(i)));
                    //    break;

                    default:
                        // warn:not support INTERVAL etc: Types.JAVA_OBJECT
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.UNSUPPORTED_TYPE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                metaData.getColumnName(i),
                                                metaData.getColumnType(i),
                                                metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                // TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            recordSender.sendToWriter(record);
            return record;
        }
    }
}
