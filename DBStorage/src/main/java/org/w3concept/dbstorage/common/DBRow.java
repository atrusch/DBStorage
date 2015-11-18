package org.w3concept.dbstorage.common;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * One data row
 * 
 * @author Alexandre Trusch
 *
 */
@JsonSerialize(using = DBRowSerializer.class)
@JsonDeserialize(using = DBRowDeserializer.class)
public class DBRow {
	protected DBMetaDataSet _MetaData;
	protected TreeMap<String, DBColumn> _RowData;
	protected DBRowStatus _Status = DBRowStatus.Unchanged;
	protected DBRowCollection _Parent;
	
	protected DBRow()
	{
		_MetaData = new DBMetaDataSet();
		_RowData = new TreeMap<>();
		_Parent = null;
	}
	
	/**
	 * Create a DBRow from p_set
	 * @param p_conn
	 * @param p_set jdbc result set
	 * @throws DBException
	 */
	protected DBRow(ResultSet p_set) throws DBException
	{
		_MetaData = new DBMetaDataSet();
		_RowData = new TreeMap<String, DBColumn>(String.CASE_INSENSITIVE_ORDER);
		_Parent = null;
		
		try
		{
			ResultSetMetaData metaData = p_set.getMetaData();

			for (int i = 1; i <= metaData.getColumnCount(); i++)
			{
				DBColumnMetaData colMeta = new DBColumnMetaData(metaData.getColumnName(i), metaData.getColumnType(i));
				
				_MetaData.addColumnMetaData(colMeta.getName(), colMeta);
				_RowData.put(colMeta.getName(), DBTools.newDBValue(colMeta, this,  p_set.getObject(i)));
			}			
		} 
		catch (Exception e)
		{
			throw new DBException(e, "unable to create DBRow object");
		}
	}
	
	protected DBRow(DBRowCollection p_collection, DBRow p_src) throws DBException
	{
		_Parent = p_collection;
		_MetaData = p_collection.getMetaData();
		_RowData = new TreeMap<String, DBColumn>(String.CASE_INSENSITIVE_ORDER);
		
		try
		{
			if(p_src != null)
				_Status = p_src._Status;
			else
				_Status = DBRowStatus.Added;
			
			Set<String> colList= _MetaData.getColumnNameList();

			for (String colName : colList)
			{
				DBColumnMetaData colMeta = _MetaData.getColumnMetaData(colName);
				DBColumn nval = DBTools.newDBValue(colMeta, this, null);

				if(p_src != null)
				{
					DBColumn dbval = p_src.getColumn(colMeta.getName());
					if(dbval != null)
					{
						nval._IsDirty = dbval._IsDirty;
						nval._OldValue = dbval._OldValue;
						nval._Value = dbval._Value;
					}
				}
				
				_RowData.put(colMeta.getName(), nval);
			}			
		} 
		catch (Exception e)
		{
			throw new DBException(e, "unable to create DBRow object");
		}
	}
	
	/**
	 * Copy p_src value in the current row. Take only the column value that exist in the current Row
	 * @param p_src
	 * @throws DBException
	 */
	public void copyValue(DBRow p_src) throws DBException
	{
		Set<String> colList= _MetaData.getColumnNameList();

		for (String colName : colList)
		{
			DBColumnMetaData colMeta = _MetaData.getColumnMetaData(colName);
			DBColumn dbval = p_src.getColumn(colMeta.getName());
			
			if(dbval != null)
				_RowData.get(colMeta.getName()).setValue(dbval.getValue());
		}			
	}
	
	/**
	 * Set status row to deleted
	 */
	public void delete()
	{
		_Status = DBRowStatus.Deleted;
	}
	
	/**
	 * Get the current row status
	 * @return
	 */
	public DBRowStatus getStatus()
	{
		return _Status;
	}
	
	/**
	 * Set the status row to unchanged
	 */
	protected void commit()
	{
		_Status = DBRowStatus.Unchanged;
	}
	
	/**
	 * Set the current row status to p_status
	 * @param p_status
	 */
	protected void setStatus(DBRowStatus p_status)
	{
		_Status = p_status;
	}
	
	/**
	 * Get the current row meta data
	 * @return
	 */
	public DBMetaDataSet getMetaData()
	{
		return _MetaData;
	}
	
	/**
	 * Get the DBValue for the column with name p_colName
	 * @param p_colName
	 * @return
	 */
	public DBColumn getColumn(String p_colName)
	{
		return _RowData.get(p_colName);
	}
	
	/**
	 * String value of the current column (debug purpose)
	 */
	public String toString()
	{
		return _RowData.toString();
	}
}
