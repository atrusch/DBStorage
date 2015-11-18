package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DBColumnMetaDataSerializer extends JsonSerializer<DBColumnMetaData> 
{
	@Override
	public void serialize(DBColumnMetaData p_metaData, JsonGenerator p_gen, SerializerProvider p_prov)
			throws IOException, JsonProcessingException 
	{
		p_gen.writeStartObject();
		p_gen.writeNumberField("Type", p_metaData._Type);
		p_gen.writeStringField("ColName", p_metaData._ColName);
		p_gen.writeBooleanField("BelongToPK", p_metaData._belongToPK);
		p_gen.writeEndObject();
	}

}
