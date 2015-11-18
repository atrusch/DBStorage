package org.w3concept.dbstorage.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DBRowSerializer  extends JsonSerializer<DBRow> 
{
	@Override
	public void serialize(DBRow p_row, JsonGenerator p_gen, SerializerProvider p_prov)
			throws IOException, JsonProcessingException 
	{
		boolean root = p_gen.getOutputContext().inRoot(); 
		
		p_gen.writeStartObject();
		p_gen.writeNumberField("Status", p_row._Status.getValue());
		
		if(root)
			p_gen.writeObjectField("RowDefinition", p_row._MetaData);
		
		p_gen.writeObjectField("Row", p_row._RowData);
		p_gen.writeEndObject();
	}

}