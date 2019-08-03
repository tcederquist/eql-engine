package com.ias.language.objects;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base object of all EQL objects
 * @author tim_c
 *
 */
public class EQLObject implements Serializable, Closeable {
	private static final long serialVersionUID = 1L;
	private final static Logger log = LoggerFactory.getLogger(EQLObject.class.getCanonicalName());

	public enum types {empty, string, integer, decimal, cursor, statement, rawText, variable, unk}
	
	protected types type;
	private String sRawVal;
	protected volatile ResultSet cursor;	// Does not serialize - transient
	protected volatile Statement cursor_stmt;
	protected volatile ResultSetMetaData rsmd;
	protected List<Map<String,Object>> cursor_cache;
	protected List<String> cursor_cols;
	protected List<String> cursor_labels;
	
	public EQLObject() {
		this.type = types.empty;
		this.sRawVal = null;
		this.cursor = null;
		this.rsmd = null;
	}
	
	public EQLObject(String val) {
		this.cursor = null;
		this.rsmd = null;
		this.sRawVal = val;
		this.type = this.getTypeFromSVal();
	}
	
	public EQLObject(String val, types type) {
		this.cursor = null;
		this.rsmd = null;
		this.sRawVal = val;
		this.type = type;
	}
	
	public EQLObject(Statement st, ResultSet rs) {
		this.cursor = rs;
		this.cursor_stmt = st;
		this.cursor_cache = new ArrayList<Map<String,Object>>();
		this.cursor_cols = new ArrayList<String>();
		this.cursor_labels = new ArrayList<String>();
		this.sRawVal = null;
		this.rsmd = null;
		this.type = types.cursor;
	}
	
	public ResultSet getCursor() {
		return this.cursor;
	}
	
	public Statement getStatement() {
		return this.cursor_stmt;
	}

	public types getType() {
		return this.type;
	}
	
	public String toString() {
		if (sRawVal != null) {
			if (type == types.string) {
				if (sRawVal.startsWith("'") && sRawVal.endsWith("'")) {
					return sRawVal.substring(1, sRawVal.length() - 1);
				} else {
					log.warn("String returned didn't have matching quotes start and end of value (continuing):" + sRawVal);
					return sRawVal;
				}
			} else if (type == types.rawText) {
				if (sRawVal.startsWith("/") && sRawVal.endsWith("/")) {
					return sRawVal.substring(1, sRawVal.length() - 1);
				} else {
					log.warn("rawString returned didn't have matching slash at start and end of value (continuing):" + sRawVal);
					return sRawVal;
				}
			}
			return sRawVal;
		} 
		return null;
	}

	public int toInt() {
		return Integer.parseInt(sRawVal);
	}
	
	public double toDouble() {
		return Double.parseDouble(sRawVal);
	}
	
	public String printType() {
		switch (this.type) {
			case statement:
				return "Stmt";
			case string:
				return "Str";
			case integer:
				return "Int";
			case rawText:
				return "RawTxt";
			case variable:
				return "Var";
			case decimal:
				return "Dec";
			case empty:
				return "Empty";
			case cursor:
				return "Cursor";
			default:
				return "Unknown";
		}
	}
	
	private types getTypeFromSVal() {
		if (sRawVal != null) {
			if (NumberUtils.isCreatable(sRawVal)) {
				if (sRawVal.contains("."))
					return types.decimal;
				else
					return types.integer;
			}
			if (sRawVal.startsWith("'") || sRawVal.startsWith("\"")) {
				return types.string;
			} else if (sRawVal.startsWith("/")) {
				return types.rawText;
			} else if (sRawVal.startsWith("?")) {	// @var on the right side is masked to ? to account for consistent substitution rules
				return types.variable;
			} else {
				if (sRawVal.length() > 0 && Character.isLetter(sRawVal.charAt(0))) {	// Select type statement
					return types.statement;
				} else {
					return types.unk;
				}
			}
		}
		return types.empty;
	}

	public void setsRawVal(String sRawVal) {
		this.sRawVal = sRawVal;
		this.type = this.getTypeFromSVal();
	}

	@Override
	public void close() throws IOException {
		try {
			if (this.cursor != null && !this.cursor.isClosed())
				this.cursor.close();
		} catch (SQLException e) {
			log.warn("Error closing cursor - continueing");
			e.printStackTrace();
		}
		try {
			if (this.cursor_stmt != null && !this.cursor_stmt.isClosed())
				this.cursor_stmt.close();
		} catch (SQLException e) {
			log.warn("Error closing cursor - continueing");
			e.printStackTrace();
		}
		this.cursor_cache.clear();
	}

	public List<Map<String, Object>> getCursor_cache() {
		return cursor_cache;
	}

	public List<String> getCursor_cols() {
		return cursor_cols;
	}

	public List<String> getCursor_labels() {
		return cursor_labels;
	}

	public ResultSetMetaData getRsmd() {
		return rsmd;
	}

	public void setRsmd(ResultSetMetaData rsmd) {
		this.rsmd = rsmd;
	}
}
