package org.w3concept.dbstorage.common;

public enum DBRowStatus {
	Added("added",1), Deleted("deleted",2), Unchanged("unchanged",3), Modified("modified",4);
	
	private String _text;
	private int _value;
	
	private DBRowStatus(String p_text,int p_value) {
		_text = p_text;
		_value = p_value;
	}

	public int getValue()
	{
		return _value;
	}
	
	public String getName()
	{
		return _text;
	}
	
	public static DBRowStatus getStatus(int p_value)
	{
		for(DBRowStatus cur : DBRowStatus.values())
			if(cur._value == p_value)
				return cur;
		
		return null;
	}
}
