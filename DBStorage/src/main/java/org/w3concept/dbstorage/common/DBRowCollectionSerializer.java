package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DBRowCollectionSerializer extends JsonSerializer<DBRowCollection> 
{
	@Override
	public void serialize(DBRowCollection p_collection, JsonGenerator p_gen, SerializerProvider p_prov)
			throws IOException, JsonProcessingException 
	{
		p_gen.writeStartObject();
		p_gen.writeObjectField("MetaData", p_collection._MetaData);
		p_gen.writeObjectField("Rows", p_collection._Rows);
		p_gen.writeEndObject();
	}
}
