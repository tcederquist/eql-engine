package com.ias.language.objects;

/**
 * Boolean defaults to True, all compares are the native value vs the supplied list. All tests will shortcut failures and not resolve any additional tests
 * 0 == false, 1 == true. LT/GT are resolved with numeric rules
 * @author tim_c
 *
 */
public class EQLBoolean extends EQLObject {
	private static final long serialVersionUID = -5126438452677055192L;
	protected boolean val;
	
	public EQLBoolean() {
		val = true;
	}
	
	public EQLBoolean(boolean state) {
		val = state;
	}
	
	public boolean eq(boolean ... test) {
		for(boolean v : test) {
			if (val != v) return false;
		}
		return true;
	}
	
	public boolean lt(boolean ... test) {
		if (val == true) return false;
		for(boolean v : test) {
			if (!v) return false;
		}
		return true;
	}
	
	public boolean gt(boolean ... test) {
		if (val == false) return false;
		for(boolean v : test) {
			if (v) return false;
		}
		return true;
	}
	
	public boolean ne(boolean ... test) {
		for(boolean v : test) {
			if (val == v) return false;
		}
		return true;
	}
	
	public boolean or(boolean ... test) {
		if (val) return true;
		for(boolean v : test) {
			if (v) return true;
		}
		return false;
	}

	public boolean xor(boolean ... test) {
		return ne(test);
	}
	
	public boolean and(boolean ... test) {
		if (!val) return false;
		for(boolean v : test) {
			if (!v) return false;
		}
		return true;
	}
	
	public boolean not() {
		return !val;
	}
}
