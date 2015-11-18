package org.w3concept.dbstorage.common;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBMetaDataSetDeserializer extends JsonDeserializer<DBMetaDataSet> 
{
	@Override
	public DBMetaDataSet deserialize(JsonParser p_parser, DeserializationContext p_ctx) 
			throws IOException, JsonProcessingException 
	{
		DBMetaDataSet res = new DBMetaDataSet();
		JsonNode node = p_parser.getCodec().readTree(p_parser);

		Iterator<JsonNode> l= node.get("ColumnsMetaData").iterator();
		ObjectMapper mapper = new ObjectMapper();
		
		while(l.hasNext())
		{
			JsonNode n = l.next();
			DBColumnMetaData colDef = mapper.treeToValue(n, DBColumnMetaData.class);
			res._MetaData.put(colDef._ColName, colDef);
			
			if(colDef._belongToPK)
				res._Keys.put(colDef._ColName, colDef);
		}
		
		res._TableName = node.get("TableName").textValue();
		
		return res;
	}
}
