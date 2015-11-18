package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DBColumnSerializer extends JsonSerializer<DBColumn> 
{
	@Override
	public void serialize(DBColumn p_column, JsonGenerator p_gen, SerializerProvider p_prov)
			throws IOException, JsonProcessingException 
	{
		p_gen.writeStartObject();
		p_gen.writeBooleanField("isDirty", p_column._IsDirty);
		p_gen.writeObjectField("value", p_column._Value);
		p_gen.writeObjectField("oldValue", p_column._OldValue);
		p_gen.writeEndObject();
	}

}
