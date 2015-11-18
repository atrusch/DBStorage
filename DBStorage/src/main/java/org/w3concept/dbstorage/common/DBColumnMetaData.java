package org.w3concept.dbstorage.common;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Contains the current column meta data
 * 
 * @author Alexandre Trusch
 *
 */
@JsonSerialize(using = DBColumnMetaDataSerializer.class)
@JsonDeserialize(using = DBColumnMetaDataDeserializer.class)
public class DBColumnMetaData {
	protected int _Type;
	protected String _ColName;
	protected boolean _belongToPK;
	
	protected DBColumnMetaData(String p_colName, int p_type) 
	{
		_Type = p_type;
		_ColName = p_colName;
	}
	
	/**
	 * Set the flag to say that this column belong to the primary key
	 */
	protected void setBelongToPK()
	{
		_belongToPK = true;
	}
	
	/**
	 * Get the java.sql.Type of the column
	 * @return
	 */
	public int getType() 
	{
		return _Type;
	}
	
	/**
	 * Get the column name
	 * @return
	 */
	public String getName() 
	{
		return _ColName;
	}
	
	/**
	 * Return true if the column is member of the primary key
	 * @return
	 */
	public boolean belongToPK()
	{
		return _belongToPK;
	}
}
