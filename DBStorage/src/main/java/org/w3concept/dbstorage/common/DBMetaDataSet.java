package org.w3concept.dbstorage.common;

import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Set of column definition
 * 
 * @author Alexandre Trusch
 *
 */

@JsonSerialize(using = DBMetaDataSetSerializer.class)
@JsonDeserialize(using = DBMetaDataSetDeserializer.class)
public class DBMetaDataSet {
	protected TreeMap<String, DBColumnMetaData> _MetaData;
	protected TreeMap<String, DBColumnMetaData> _Keys;
	protected String _TableName;
	
	protected DBMetaDataSet()
	{
		_MetaData = new TreeMap<String, DBColumnMetaData>(String.CASE_INSENSITIVE_ORDER);
		_Keys = new TreeMap<String, DBColumnMetaData>(String.CASE_INSENSITIVE_ORDER);
		_TableName = null;
	}

	protected DBMetaDataSet(String p_tableName)
	{
		_MetaData = new TreeMap<String, DBColumnMetaData>(String.CASE_INSENSITIVE_ORDER);
		_Keys = new TreeMap<String, DBColumnMetaData>(String.CASE_INSENSITIVE_ORDER);
		_TableName = p_tableName;
	}
	
	/**
	 * Return the table name if this meta data defined a table. Otherwise null (always null if it come from a dbresultset)
	 * @return
	 */
	public String getTableName()
	{
		return _TableName;
	}
	
	/**
	 * Add the column meta data to the current set
	 * 
	 * @param p_columnName column name
	 * @param p_colDef definition of the column name
	 * @throws DBException throw an exception if this column is already defined in the current set
	 */
	protected void addColumnMetaData(String p_columnName, DBColumnMetaData p_colDef) throws DBException
	{
		if(_MetaData.containsKey(p_columnName))
			throw new DBException("The column '"+p_columnName+"' is already defined!");
		
		_MetaData.put(p_columnName, p_colDef);
	}
	
	/**
	 * Declare the column as a primary key
	 * 
	 * @param p_columnName column name
	 * @throws DBException throw an exception if this column is unknowed
	 */
	protected void setAsKey(String p_columnName) throws DBException
	{
		if(!_MetaData.containsKey(p_columnName))
			throw new DBException("The column '"+p_columnName+"' is unknow can not set as a primary key!");
		
		_Keys.put(p_columnName, _MetaData.get(p_columnName));
	}

	/**
	 * Get the meta data for the column p_columnName. If the column name is unkonw it return null
	 * 
	 * @param p_columnName column name
	 * @return column meta data
	 */
	public DBColumnMetaData getColumnMetaData(String p_columnName)
	{
		return _MetaData.get(p_columnName);
	}
	
	/**
	 * Get the column name list
	 * @return
	 */
	public Set<String> getColumnNameList()
	{
		return _MetaData.keySet();
	}
	
	/**
	 * Get the key name list
	 * @return
	 */
	public Set<String> getKeyNameList()
	{
		return _Keys.keySet();
	}
	
	/**
	 * Clear the set (object become unusable). Done to help the garbage collector
	 */
	public void clear() {
		_Keys.clear();
		_MetaData.clear();
		
		_Keys = null;
		_MetaData = null;
	}
}
