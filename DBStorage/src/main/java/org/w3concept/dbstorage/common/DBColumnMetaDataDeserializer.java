package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;

public class DBColumnMetaDataDeserializer extends JsonDeserializer<DBColumnMetaData> 
{
	@Override
	public DBColumnMetaData deserialize(JsonParser p_parser, DeserializationContext p_ctx) 
			throws IOException, JsonProcessingException 
	{
		JsonNode node = p_parser.getCodec().readTree(p_parser);
		
		int type = (Integer)((IntNode)node.get("Type")).numberValue();
		String colName = node.get("ColName").textValue();
		
		DBColumnMetaData res = new DBColumnMetaData(colName, type);
		res._belongToPK = node.get("BelongToPK").booleanValue();
		return res;
	}
}
