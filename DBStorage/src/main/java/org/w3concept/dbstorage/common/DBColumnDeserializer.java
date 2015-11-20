package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class DBColumnDeserializer extends JsonDeserializer<DBColumn> 
{
	private Object getValue(JsonNode p_node)
	{
		if(p_node.isNull())
			return null;
		
		return p_node.asText();
	}
	
	@Override
	public DBColumn deserialize(JsonParser p_parser, DeserializationContext p_ctx) 
			throws IOException, JsonProcessingException 
	{
		JsonNode node = p_parser.getCodec().readTree(p_parser);
		
		DBColumn res = new DBColumn();
		res._IsDirty = node.get("isDirty").booleanValue();
		res._Value = getValue(node.get("value"));
		res._OldValue = getValue(node.get("oldValue"));
		
		return res;
	}
}