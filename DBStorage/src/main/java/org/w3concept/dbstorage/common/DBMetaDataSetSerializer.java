package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DBMetaDataSetSerializer extends JsonSerializer<DBMetaDataSet> 
{
	@Override
	public void serialize(DBMetaDataSet p_metaData, JsonGenerator p_gen, SerializerProvider p_prov)
			throws IOException, JsonProcessingException 
	{
		p_gen.writeStartObject();
		p_gen.writeObjectField("ColumnsMetaData", p_metaData._MetaData);
		//p_gen.writeObjectField("KeysMetaData", p_metaData._Keys);
		p_gen.writeStringField("TableName", p_metaData._TableName);
		p_gen.writeEndObject();
	}

}
