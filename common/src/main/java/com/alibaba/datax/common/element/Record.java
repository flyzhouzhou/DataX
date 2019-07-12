package com.alibaba.datax.common.element;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {

	public void addColumn(Column column);

	public void setColumn(int i, final Column column);

	public Column getColumn(int i);

	public String toString();

	public int getColumnNumber();

	public int getByteSize();

	public int getMemorySize();

	public void setDbInstance(String dbInstance);

	public String getDbInstance();

	public void setCurrentTable(String currentTable);

	public String getCurrentTable();

	public String getColumnNames(int index);

	public void setColumnNames(String columnName);

	public String getColumnTypes(int index);

	public void setColumnTypes(String columnType);

}
