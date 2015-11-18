package org.w3concept.dbstorage.common;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * This class contain the value of one db column.
 * @author Alexandre Trusch
 *
 */
@JsonSerialize(using = DBColumnSerializer.class)
@JsonDeserialize(using = DBColumnDeserializer.class)
public class DBColumn {
	protected DBColumnMetaData _MetaData;
	protected Object _OldValue;
	protected Object _Value;
	protected boolean _IsDirty;
	protected DBRow _Row;

	/**
	 * Only used for deserialisation
	 */
	protected DBColumn()
	{
	}
	
	protected DBColumn(DBRow p_row, DBColumnMetaData p_metaData, Object p_value) 
	{
		_MetaData = p_metaData;
		_Row = p_row;

		_Value = p_value;
		_OldValue = null;		
		
		_IsDirty = false;
	}
	
	/**
	 * @return the current value
	 */
	public Object getValue() {
		return _Value;
	}

	/**
	 * @return the old value
	 */
	public Object getOldValue() {
		return _OldValue;
	}
	
	/**
	 * Get the meta data of the current column
	 * @return
	 */
	public DBColumnMetaData getMetaData() 
	{
		return _MetaData;
	}
	
	/**
	 * Set a new value and store the old one.
	 * @param p_value new value
	 * @throws DBException when the original class type and new value class type are different
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setValue(Object p_value) throws DBException {
		if(p_value != null && _Value != null && _Value instanceof Comparable)
		{
			Comparable v = (Comparable) _Value;
			if(v.compareTo(p_value)==0)
				return;
		}
		
		_OldValue = _Value;
		_IsDirty = true;
		_Row.setStatus(DBRowStatus.Modified);
		
		if(_Value != null && p_value != null && !(_Value.getClass().equals(p_value.getClass()) ))
			throw new DBException("Try to set a value of invalid type!");
		
		_Value = p_value;
	}

	/**
	 * True if the current value was modified
	 * @return
	 */
	public boolean isDirty() {
		return _IsDirty;
	}
	
	/**
	 * Set to false the isDirty Flag
	 */
	protected void commit()
	{
		_IsDirty = false;
	}
	
	/**
	 * String value of the current column (debug purpose)
	 */
	public String toString()
	{
		if(_Value != null)
			return _Value.toString();
		
		return "<null>";
	}
}
