package org.w3concept.dbstorage.common;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class DBMySQLConnection  extends DBConnection {
	
	public DBMySQLConnection(Connection p_conn,boolean p_autocommit) throws DBException
	{
		_Conn = p_conn;
		_TableMetaData = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		
		try 
		{
			_Conn.setAutoCommit(p_autocommit);
			initTable();
		} 
		catch (SQLException e) 
		{
			throw(new DBException(e, "Unable to init table metadata"));
		}
	}
	
	private void initTable() throws SQLException, DBException
	{
		String[] elements = {"TABLE"};
		DatabaseMetaData md = _Conn.getMetaData();
		ResultSet rs = md.getTables(null,md.getUserName(), "%", elements);
		
		while (rs.next()) 
		{
			String tableName = rs.getString(3);
			initMetaDataTable(md, tableName);
		}
		
		rs.close();
	}

	private void initMetaDataTable(DatabaseMetaData p_md, String p_table) throws SQLException, DBException
	{
		DBMetaDataSet set = new DBMetaDataSet(p_table);
		_TableMetaData.put(p_table, set);
		
		ResultSet rs = p_md.getColumns(null, p_md.getUserName(), p_table, "%");
		
		while (rs.next()) 
		{
			String colName = rs.getString(4);
			int dataType = rs.getInt(5);
			
			DBColumnMetaData colMeta = new DBColumnMetaData(colName, dataType);
			set.addColumnMetaData(colName, colMeta);
		}
		
		rs.close();
		initKeysTable(p_md, set, p_table);
	}

	private void initKeysTable(DatabaseMetaData p_md, DBMetaDataSet p_set, String p_table) throws SQLException, DBException
	{
		ResultSet rs = p_md.getPrimaryKeys(null, p_md.getUserName(), p_table);
		
		while (rs.next()) 
		{
			String colName = rs.getString(4);
			p_set.getColumnMetaData(colName).setBelongToPK();
			p_set.setAsKey(colName);
		}
		
		rs.close();
	}
	
	private void setParameters(PreparedStatement p_stmt, int p_startPos, DBRow p_data, Set<String> p_colNameParameter)	throws DBException
	{
		int paramPos = p_startPos;
		
		try
		{
			for (String colName : p_colNameParameter)
			{
				DBColumn col = p_data.getColumn(colName);
				Object value = col == null ? null : col.getValue();

				if (value == null)
					p_stmt.setNull(++paramPos, col.getMetaData().getType());
				else
				{
					switch (col.getMetaData().getType()) {
					case Types.CLOB: {
						StringReader r = new StringReader((String)value);
						p_stmt.setCharacterStream(++paramPos, r);
						break;
					}
					case Types.BLOB: {
						ByteArrayInputStream r = new ByteArrayInputStream((byte[])value);
						p_stmt.setBinaryStream(++paramPos, r);
						break;
					}
					default:
						p_stmt.setObject(++paramPos, value);
						break;
					}
				}
			}
		}
		catch (Exception e)
		{
			throw new DBException(e, "unable to set query paramater (#"+paramPos+")");
		}
	}

	private void getGeneratedKeys(DBMetaDataSet p_tableDef, PreparedStatement stmt, DBRow p_data) throws SQLException, DBException
	{
		ResultSet rs = null;
		
		try
		{
			rs = stmt.getGeneratedKeys();

			if (rs.next())
			{
				ResultSetMetaData metaData = rs.getMetaData();
				int colCount = metaData.getColumnCount();

				for (int i = 1; i <= colCount; i++)
				{
					String colName = metaData.getColumnName(i);

					if (colName.equals("GENERATED_KEY"))
					{
						Long rowid = rs.getLong(i);
						Set<String> keys = p_tableDef.getKeyNameList();
						
						for(String keyName : keys)
						{
							p_data.getColumn(keyName).setValue(rowid);
							return;
						}	
					}
					else
					{
						DBColumn col = p_data.getColumn(colName);
						col.setValue(rs.getObject(i));
					}
				}
			}
		}
		finally
		{
			if(rs != null) rs.close();
		}
	}

	private String deleteCmd(DBMetaDataSet p_tableDef)
	{
		String cmd = "delete from " + p_tableDef.getTableName();
		String cmdWhere = "";

		for (String colName : p_tableDef.getKeyNameList())
			cmdWhere += (cmdWhere.length() > 0 ? " and " : "") + colName+"=?";

		return cmd + " where " + cmdWhere ;
	}

	private String updateCmd(DBMetaDataSet p_tableDef)
	{
		String cmd = "update " + p_tableDef.getTableName();
		String cmdWhere = "";
		String cmdSet = "";

		for (String colName : p_tableDef.getColumnNameList())
		{
			DBColumnMetaData colMeta = p_tableDef.getColumnMetaData(colName);
			if(colMeta.belongToPK())
				cmdWhere += (cmdWhere.length() > 0 ? " and " : "") + colName+"=?";
			else
				cmdSet += (cmdSet.length() > 0 ? ", " : "") + colName+"=?";
		}

		return cmd + " set " + cmdSet + " where " + cmdWhere ;
	}
	
	private String insertCmd(DBMetaDataSet p_tableDef, DBRow p_data)
	{
		String cmd = "insert into " + p_tableDef.getTableName();
		String cmdColName = "";
		String cmdVal = "";

		for (String colName : p_tableDef.getColumnNameList())
		{
			cmdColName += (cmdColName.length() > 0 ? "," : "") + colName;
			cmdVal += (cmdVal.length() > 0 ? "," : "") + "?";
		}

		return cmd + " (" + cmdColName + ") values (" + cmdVal + ")";
	}
	
	/**
	 * Insert in DB all the rows of the collection with the status Added
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public void insert(String p_tableName, DBRowCollection p_data) throws DBException
	{
		String cmdInsert = null;
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		boolean isDBTable = p_data instanceof DBTable; 
		
		try 
		{
			for(DBRow row : p_data._Rows)
			{
				if(row.getStatus().equals(DBRowStatus.Added))
				{
					if(cmdInsert == null)
					{
						cmdInsert = insertCmd(tableDef, row);
						stmt = _Conn.prepareStatement(cmdInsert, Statement.RETURN_GENERATED_KEYS);
					}

					setParameters(stmt, 0, row, tableDef.getColumnNameList());
					stmt.execute();
					getGeneratedKeys(tableDef, stmt, row);
					
					if(isDBTable)
						row.commit();
					
					stmt.clearParameters();					
				}
			}
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to insert data\nquery : "+cmdInsert+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}

	/**
	 * Delete in DB all the rows of the collection with the status Deleted
	 * @param p_tableName
	 * @param p_data
	 * @throws DBException
	 */
	public void delete(String p_tableName, DBRowCollection p_data) throws DBException
	{
		String cmdDelete = null;
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		boolean isDBTable = p_data instanceof DBTable; 
		
		try 
		{
			for(DBRow row : p_data._Rows)
			{
				if(row.getStatus().equals(DBRowStatus.Deleted))
				{
					if(cmdDelete == null)
					{
						cmdDelete = deleteCmd(tableDef);
						stmt = _Conn.prepareStatement(cmdDelete);
					}

					setParameters(stmt, 0, row, tableDef.getKeyNameList());
					stmt.execute();
					
					if(isDBTable)
						row.commit();
					
					stmt.clearParameters();					
				}
			}
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to insert data\nquery : "+cmdDelete+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}
	
	public void update(String p_tableName, DBRowCollection p_data) throws DBException
	{
		String cmdUpdate = null;
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		boolean isDBTable = p_data instanceof DBTable; 
		
		try 
		{
			HashSet<String> updCol = new HashSet<>();
			
			for(String colName : tableDef.getColumnNameList())
			{
				DBColumnMetaData colMeta = tableDef.getColumnMetaData(colName);
				if(!colMeta.belongToPK())
					updCol.add(colName);
			}

			for(DBRow row : p_data._Rows)
			{
				if(row.getStatus().equals(DBRowStatus.Modified))
				{
					if(cmdUpdate == null)
					{
						cmdUpdate = updateCmd(tableDef);
						stmt = _Conn.prepareStatement(cmdUpdate);
					}

					setParameters(stmt, 0, row, updCol);
					setParameters(stmt, updCol.size(), row, tableDef.getKeyNameList());
					stmt.execute();
					
					if(isDBTable)
						row.commit();
					
					stmt.clearParameters();					
				}
			}
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to insert data\nquery : "+cmdUpdate+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}

	public void insert(String p_tableName, DBRow p_data) throws DBException
	{
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		String cmdInsert = insertCmd(tableDef, p_data);
		
		try 
		{
			stmt = _Conn.prepareStatement(cmdInsert, Statement.RETURN_GENERATED_KEYS);
			setParameters(stmt, 0, p_data, tableDef.getColumnNameList());
			stmt.execute();
			getGeneratedKeys(tableDef, stmt, p_data);
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to insert data\nquery : "+cmdInsert+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}

	public void delete(String p_tableName, DBRow p_data) throws DBException
	{
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		String cmdDelete = deleteCmd(tableDef);
		
		try 
		{
			stmt = _Conn.prepareStatement(cmdDelete);
			setParameters(stmt, 0, p_data, tableDef.getKeyNameList());
			stmt.execute();
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to delete data\nquery : "+cmdDelete+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}

	public void update(String p_tableName, DBRow p_data) throws DBException
	{
		PreparedStatement stmt = null;
		DBMetaDataSet tableDef = _TableMetaData.get(p_tableName);
		String cmdUpdate = updateCmd(tableDef);
		
		try 
		{
			HashSet<String> updCol = new HashSet<>();
			
			for(String colName : tableDef.getColumnNameList())
			{
				DBColumnMetaData colMeta = tableDef.getColumnMetaData(colName);
				if(!colMeta.belongToPK())
					updCol.add(colName);
			}
			
			stmt = _Conn.prepareStatement(cmdUpdate);
			setParameters(stmt, 0, p_data, updCol);
			setParameters(stmt, updCol.size(), p_data, tableDef.getKeyNameList());
			stmt.executeUpdate();
		} 
		catch (Throwable e) 
		{
			throw new DBException(e,"Unable to delete data\nquery : "+cmdUpdate+"\ndata :"+p_data);
		}
		finally
		{
			if(stmt != null) DBTools.closeStatement(stmt);
		}
	}
}
