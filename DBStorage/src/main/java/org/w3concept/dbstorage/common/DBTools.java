package org.w3concept.dbstorage.common;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.io.IOUtils;

public class DBTools {
	protected static DBColumn newDBValue(DBColumnMetaData p_meta, DBRow p_row, Object p_value) throws DBException
	{
		Object value = null;
		
		if(p_value == null)
			return new DBColumn(p_row, p_meta, value);
		
		switch (p_meta.getType()) {
		case Types.CLOB:
			try
			{
				Clob b = (Clob) p_value;
				StringWriter wr = new StringWriter();
				IOUtils.copy(b.getCharacterStream(), wr);
				wr.close();

				value = wr.toString();
			}
			catch (Exception e)
			{
				throw new DBException(e, "unable to create clob object");
			}
			break;
		case Types.BLOB:
			try
			{
				Blob b = (Blob) p_value;

				ByteArrayOutputStream os = new ByteArrayOutputStream();
				IOUtils.copy(b.getBinaryStream(), os);
				os.close();
				
				value = os.toByteArray();
			}
			catch (Exception e)
			{
				throw new DBException(e, "unable to create clob object" );
			}
			break;
		default :
			value = p_value;
		}
		
		return new DBColumn(p_row, p_meta, value);
	}
	
	/**
	 * Close the statement (skip all exception)
	 * @param p_stmt
	 */
	public static void closeStatement(Statement p_stmt)
	{
		if(p_stmt == null)
			return;
		
		try {
			p_stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Close the result set (skip all exception)
	 * @param p_set
	 */
	public static void closeResultSet(ResultSet p_set)
	{
		if(p_set == null)
			return;
		
		try {
			p_set.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
