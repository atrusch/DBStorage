package org.w3concept.dbstorage.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TreeMap;

public abstract class DBConnection {
	protected Connection _Conn;
	protected TreeMap<String, DBMetaDataSet> _TableMetaData;
	
	protected Connection getConnection()
	{
		return _Conn;
	}

	public DBMetaDataSet getTableMetaData(String p_table)
	{
		return _TableMetaData.get(p_table);
	}
	
	public DBResultSet createResultSet(String p_query) throws DBException
	{
		DBResultSet rs = new DBResultSet(this, p_query);
		
		return rs;
	}
	
	/**
	 * Insert the content of the row in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void insert(String p_tableName, DBRow p_data) throws DBException;
	
	/**
	 * Insert all rows of the collection with the status Added in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void insert(String p_tableName, DBRowCollection p_data) throws DBException;
	
	/**
	 * Delete the row in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void delete(String p_tableName, DBRow p_data) throws DBException;
	
	/**
	 * Delete all rows of the collection with the status Deleted in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void delete(String p_tableName, DBRowCollection p_data) throws DBException;
	
	/**
	 * Update the row in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void update(String p_tableName, DBRow p_data) throws DBException;
	
	/**
	 * Update all rows of the collection with the status Modified in the table p_tableName
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public abstract void update(String p_tableName, DBRowCollection p_data) throws DBException;
	
	/**
	 * Close the connection
	 * @throws DBException
	 */
	public void close() throws DBException
	{
		try 
		{
			_Conn.close();
		} 
		catch (SQLException e) 
		{
			throw new DBException(e,"Unable to close connection");
		}
	}
	
	/**
	 * Rollback the transaction
	 * @throws DBException
	 */
	public void rollback() throws DBException
	{
		try {
			_Conn.rollback();
		} catch (SQLException e) {
			throw new DBException(e,"Rollback error");
		}
	}

	/**
	 * Commit the transaction
	 * @throws DBException
	 */
	public void commit() throws DBException
	{
		try {
			_Conn.commit();
		} catch (SQLException e) {
			throw new DBException(e,"Commit error");
		}
	}
	
	/**
	 * Get the jdbc connection
	 * @return
	 */
	public Connection getJDBCConnection()
	{
		return _Conn;
	}
	
	/**
	 * Execute a SQL command
	 * @param p_cmd
	 * @throws DBException
	 */
	public void execute(String p_cmd) throws DBException
	{
		PreparedStatement stmt = null;
		
		try 
		{
			stmt = _Conn.prepareStatement(p_cmd);
			stmt.execute();
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to exectue command\nquery : "+p_cmd);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}
}