package org.w3concept.dbstorage.common;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBRowCollectionDeserializer extends JsonDeserializer<DBRowCollection> 
{
	@Override
	public DBRowCollection deserialize(JsonParser p_parser, DeserializationContext p_ctx)
			throws IOException, JsonProcessingException 
	{
		JsonNode node = p_parser.getCodec().readTree(p_parser);
		JsonNode metaNode = node.get("MetaData");
		
		ObjectMapper mapper = new ObjectMapper();
		try
		{
			DBRowCollection res = new DBRowCollection(mapper.treeToValue(metaNode, DBMetaDataSet.class));
			JsonNode rowListNode = node.get("Rows");
			Iterator<JsonNode> nl = rowListNode.elements();
			
			while(nl.hasNext())
			{
				JsonNode rowNode = nl.next();
				DBRow row = mapper.treeToValue(rowNode, DBRow.class);
				row._MetaData = res._MetaData;
				
				for(String colName : row._RowData.keySet())
				{
					DBColumn col = row.getColumn(colName);
					
					col._MetaData = row._MetaData.getColumnMetaData(colName);
					col._OldValue = DBRowDeserializer.getValue(col._MetaData._Type, (String)col._OldValue);
					col._Value = DBRowDeserializer.getValue(col._MetaData._Type, (String)col._Value);
				}
				
				res._Rows.add(row);
			}
			
			return res;
		}
		catch(Exception e)
		{
			throw new IOException(e);
		}
	}

}
