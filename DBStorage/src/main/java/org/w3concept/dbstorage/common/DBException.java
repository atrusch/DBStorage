package org.w3concept.dbstorage.common;

public class DBException extends Exception {
	private static final long serialVersionUID = 1110216396178761504L;

	public DBException(Throwable p_ex, String p_message) {
		super(p_message,p_ex);
	}
	
	public DBException(String p_message) {
		super(p_message);
	}	
}
