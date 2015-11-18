package org.w3concept.dbstorage.common;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;

import org.joda.convert.StringConvert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBRowDeserializer extends JsonDeserializer<DBRow> 
{
	@SuppressWarnings("unchecked")
	public static Object getValue(int p_type,String p_value) throws ClassNotFoundException
	{
		if(p_value == null)
			return null;
		
		String className = SQLTypeMap.convert(p_type);
		
		if(String.class.getName().equals(className))
			return p_value;
		
		@SuppressWarnings("rawtypes")
		Class cl = Class.forName(className);
		if(p_value.length()>0)
		{
			if("java.sql.Timestamp".equals(className))
				return new Timestamp(Long.parseLong(p_value));
			else
				return StringConvert.INSTANCE.convertFromString(cl, p_value);
		}
		
		return null;
	}
	
	@Override
	public DBRow deserialize(JsonParser p_parser, DeserializationContext p_ctx) throws IOException, JsonProcessingException 
	{
		DBRow res = new DBRow();
		ObjectMapper mapper = new ObjectMapper();
		
		JsonNode node = p_parser.getCodec().readTree(p_parser);
		res._Status = DBRowStatus.getStatus(node.get("Status").intValue());
		
		if(node.has("RowDefinition"))
			res._MetaData = mapper.treeToValue(node.get("RowDefinition"), DBMetaDataSet.class);
		else
			res._MetaData = null;

		JsonNode rowNode = node.get("Row");
		Iterator<String> l= rowNode.fieldNames();
		
		Iterator<DBColumnMetaData> kl = res._MetaData != null ? res._MetaData._MetaData.values().iterator() : null;
		
		try
		{
			while(l.hasNext())
			{
				String colName = l.next();
				DBColumn col = mapper.treeToValue(rowNode.get(colName), DBColumn.class);
				
				col._Row = res;
				
				if(kl != null)
				{
					DBColumnMetaData colDef = kl.next();
					col._MetaData = colDef;
					col._Value = getValue(col._MetaData._Type, (String)col._Value);
					col._OldValue = getValue(col._MetaData._Type, (String)col._OldValue);
				}
				
				res._RowData.put(colName, col);
			}
		}
		catch(ClassNotFoundException e)
		{
			throw new IOException(e);
		}
		
		return res;
	}

}
