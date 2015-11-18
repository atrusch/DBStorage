package org.w3concept.dbstorage.common;

import java.sql.Types;

public class SQLTypeMap {

	/**
	 * Translates a data type from an integer (java.sql.Types value) to a string
	 * that represents the corresponding class.
	 * 
	 * @param type
	 *            The java.sql.Types value to convert to a string
	 *            representation.
	 * @return The class name that corresponds to the given java.sql.Types
	 *         value, or "java.lang.Object" if the type has no known mapping.
	 */
	public static String convert(int type)
	{
		String result = "java.lang.Object";

		switch (type) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.CLOB:
			result = "java.lang.String";
			break;

		case Types.NUMERIC:
		case Types.DECIMAL:
			result = "java.math.BigDecimal";
			break;

		case Types.BIT:
			result = "java.lang.Boolean";
			break;

		case Types.TINYINT:
			result = "java.lang.Byte";
			break;

		case Types.SMALLINT:
			result = "java.lang.Short";
			break;

		case Types.INTEGER:
			result = "java.lang.Integer";
			break;

		case Types.BIGINT:
			result = "java.lang.Long";
			break;

		case Types.REAL:
			result = "java.lang.Real";
			break;

		case Types.FLOAT:
		case Types.DOUBLE:
			result = "java.lang.Double";
			break;

		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			result = "java.lang.Byte[]";
			break;

		case Types.DATE:
			result = "java.sql.Date";
			break;

		case Types.TIME:
			result = "java.sql.Time";
			break;

		case Types.TIMESTAMP:
			result = "java.sql.Timestamp";
			break;
		}

		return result;
	}
}