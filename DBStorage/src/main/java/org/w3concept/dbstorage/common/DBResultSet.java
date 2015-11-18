package org.w3concept.dbstorage.common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Equivalent to jdbc ResultSet with less functions
 * 
 * @author Alexandre Trusch
 *
 */
public class DBResultSet {
	Statement _Stm;
	ResultSet _Res;
	DBConnection _Conn;
	
	/**
	 * Create the result set for the query
	 * @param p_conn
	 * @param p_query SQL query
	 * @throws DBException
	 */
	public DBResultSet(DBConnection p_conn, String p_query) throws DBException
	{
		try
		{
			_Conn = p_conn;
			_Stm = p_conn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			_Res = _Stm.executeQuery(p_query);
		} 
		catch (SQLException e)
		{
			throw (new DBException(e, e.getMessage()));
		}	
	}
	
	/**
	 * Set the cursor to the next row 
	 * @return false if no more row
	 * @throws DBException
	 */
	public boolean next() throws DBException
	{

		try
		{
			return _Res.next();
		} 
		catch (SQLException e)
		{
			throw new DBException(e, e.getMessage());
		}
	}
	
	/**
	 * Set the cursor at p_rows position
	 * @param p_rows
	 * @return
	 * @throws DBException
	 */
	public boolean absolute(int p_rows) throws DBException
	{
		try 
		{
			return _Res.absolute(p_rows);
		} 
		catch (SQLException e) 
		{
			throw new DBException(e, e.getMessage());
		}
	}
	
	/**
	 * Set the cursor before the first row
	 * @throws DBException
	 */
	public void beforeFirst() throws DBException
	{
		try 
		{
			_Res.beforeFirst();
		} 
		catch (SQLException e) 
		{
			throw new DBException(e, e.getMessage());
		}
	}
	
	/**
	 * Get the number of rows in this result set
	 * @return row count
	 * @throws DBException
	 */
	public int count() throws DBException
	{
		int nb = 0;
		try 
		{
			int pos = _Res.getRow();
			
			if(_Res.last())
			{
				nb=_Res.getRow();
				_Res.absolute(pos);
			}
			
			return nb;
		} 
		catch (SQLException e) 
		{
			throw new DBException(e, e.getMessage());
		}
	}
	
	/**
	 * Get the current row
	 * 
	 * @return
	 * @throws DBException
	 */
	public DBRow getRow() throws DBException 
	{
		return new DBRow(_Res);
	}
	
	/**
	 * Clean the result set
	 */
	public void clean()
	{
		_Conn = null;
		
		try {
			_Res.close();
		} 
		catch (SQLException e) 
		{
		}
		
		try {
			_Stm.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Get the connection of the current result set
	 * @return
	 */
	public DBConnection getConnection()
	{
		return _Conn;
	}
}
