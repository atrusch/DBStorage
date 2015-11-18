package org.w3concept.dbstorage.common;

public class DBTable extends DBRowCollection
{
	/**
	 * Create un new table collection
	 * 
	 * @param p_conn
	 * @param p_tableName
	 * @throws DBException
	 */
	public DBTable(DBConnection p_conn, String p_tableName) throws DBException
	{
		super(p_conn.getTableMetaData(p_tableName));
	}
	
	/**
	 * Get the table name
	 * @return
	 */
	public String getName()
	{
		return _MetaData.getTableName();
	}
}
