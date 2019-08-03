package com.ias.language.eql.imp;

public class EQLException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public EQLException(String s) {
		super(s);
	}
	
	public EQLException(Exception e) {
		super(e);
	}
}
