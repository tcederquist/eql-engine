package com.ias.language.objects;

public class EQLString extends EQLObject {
	private static final long serialVersionUID = 5056564773966647574L;
	protected String val;
	
	public EQLString() {
		val = "";
	}
	
	public EQLString(String state) {
		val = state;
	}
	
	public boolean eq(String test) {
		return val.compareTo(val) == 0;
	}

	public boolean eqAny(String ... test) {
		for(String v : test) {
			if (val.compareTo(v) == 0) return true;
		}
		return false;
	}

	public boolean lt(String test) {
		return val.compareTo(test) < 0;
	}
	
	public boolean gt(String test) {
		return val.compareTo(test) > 0;
	}
	
	public boolean ne(String test) {
		return val.compareTo(test) != 0;
	}
}
