package org.w3concept.dbstorage.common;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = DBRowCollectionSerializer.class)
@JsonDeserialize(using = DBRowCollectionDeserializer.class)
public class DBRowCollection {
	protected DBMetaDataSet _MetaData;
	protected ArrayList<DBRow> _Rows;

	/**
	 * Create a new collection with all data filled from the result set. Doesn't close the result set.
	 * @param p_res
	 * @throws DBException
	 */
	public DBRowCollection(DBResultSet p_res) throws DBException
	{
		while(p_res.next())
		{
			DBRow row = p_res.getRow();
			row._Parent = this;
			
			if(_MetaData == null)
				_MetaData = row.getMetaData();
			else
				row._MetaData = _MetaData;
			
			_Rows.add(row);
		}
	}

	
	/**
	 * Create a row collection with the same definition as p_row. Does not add p_row to the collection
	 * @param p_row template row
	 */
	public DBRowCollection(DBRow p_row) throws DBException
	{
		_MetaData = p_row.getMetaData();
		_Rows = new ArrayList<>();
	}
	
	/**
	 * Create a row collection with p_meta as definition for each column
	 * @param p_meta
	 * @throws DBException
	 */
	public DBRowCollection(DBMetaDataSet p_meta) throws DBException
	{
		if(p_meta == null)
			throw new DBException("No meta data defined!");

		_MetaData = p_meta;
		_Rows = new ArrayList<>();
	}
	
	/**
	 * Create a new row in the row collection. This row is compatible with the current meta data. Unknow coluumn are set
	 * with null value. Keep the status of the orginal row
	 * 
	 * @param p_row
	 * @return the new row
	 * @throws DBException
	 */
	public DBRow importRow(DBRow p_row) throws DBException
	{
		DBRow nrow = new DBRow(this,p_row);
		nrow._Parent = this;
		
		_Rows.add(nrow);
		
		return nrow;
	}
	
	/**
	 * Create a new empty row that belong to this collection
	 * @return
	 * @throws DBException
	 */
	public DBRow newRow() throws DBException
	{
		return importRow(null);
	}
	
	public DBMetaDataSet getMetaData()
	{
		return _MetaData;
	}	
	
	/**
	 * Fill the  row collection with the dbresultset. 
	 * If some column of the collection does not exist in the result set null values are added
	 * @param p_set
	 * @throws DBException
	 */
	public void fill(DBResultSet p_set) throws DBException
	{
		ResultSet rs = p_set._Res;
		
		try
		{
			while(rs.next())
			{
				DBRow row = new DBRow(this,null);
				
				ResultSetMetaData srcMetaData = p_set._Res.getMetaData();
				row._Status = DBRowStatus.Unchanged;
				
				for (int i = 1; i<= srcMetaData.getColumnCount(); i++)
				{
					String colName = srcMetaData.getColumnName(i);
					DBColumnMetaData colMeta = _MetaData.getColumnMetaData(colName);
					
					if(colMeta!=null)
					{
						DBColumn val = DBTools.newDBValue(colMeta, row, rs.getObject(i));
						row._RowData.put(colMeta.getName(), val);
					}
				}
				
				for(DBColumnMetaData colMeta : _MetaData._MetaData.values())
				{
					DBColumn col = row.getColumn(colMeta._ColName);
					if(col == null)
					{
						col = DBTools.newDBValue(colMeta, row, null);
						row._RowData.put(colMeta.getName(), col);
					}
				}
				
				_Rows.add(row);
			}
		} 
		catch (Exception e)
		{
			throw new DBException(e, "unable to create DBRow object");
		}
	}
}
