package com.ias.language.eql.imp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.gson.internal.LinkedTreeMap;
import com.ias.language.objects.EQLInstruction;
import com.ias.language.objects.EQLObject;
import com.ias.language.objects.IASOnDemandDataSource;

public class EQLCommandLineDriver extends EQLUtilities {
	private final static Logger log = LoggerFactory.getLogger(EQLCommandLineDriver.class.getCanonicalName());

	protected Map<String, EQLObject> vars;
	protected List<EQLException> errorStack;
	protected List<EQLInstruction> instructions;
	protected Connection conn;
	protected int completedLine;
	protected int queryTimeout;

	public EQLCommandLineDriver(Properties config) {
		super(config);
		this.queryTimeout = -1;	// do nothing
		this.vars = new TreeMap<String, EQLObject>(String.CASE_INSENSITIVE_ORDER);
		String debugLvl = config.getProperty("eqlLogLevel", "2");
		try {
			this.logLevel = Integer.parseInt(debugLvl);
		} catch (NumberFormatException e) {
			log.warn("EQLEngine startup config value illegal - default to 1 - value supplied:{}", debugLvl);
		}
		this.errorStack = new ArrayList<EQLException>();
		this.instructions = null;
		this.conn = null;
		this.completedLine = 0;
	}
	
	public boolean compile(String code) throws IOException {
		try {
			this.instructions = EQLInstruction.InstructionFactory(code);
			log.debug("Compiled {} instructions", this.instructions.size());
		} catch (EQLException e) {
			this.errorMsg("compile", e.getMessage());
			return false;
		}
		return true;
	}
	
	/**
	 * Multi-line execution logic and handles variable assignments and connection instruction processing
	 * @throws EQLException
	 */
	public void run(int startingLine) {
		int lineExecuting = 0;
		for(EQLInstruction instruct:instructions) {
			lineExecuting++;
			if (lineExecuting > startingLine) {
				//TODO put the function logic into the exec function so it can handle any command handed to it
				if (instruct.getFunction().equals("var")) {
					EQLObject val = instruct.getAssignVal();
					//this.debugMsg("engine", "Assignment of '" + instruct.getAssignName() + "' to value:" + instruct.getAssignVal());
					EQLObject oldVal = this.vars.put(instruct.getAssignName(), val);
					if (oldVal != null && oldVal.getType() == EQLObject.types.cursor) {
						this.closeCursorIfLastReference(instruct.getAssignName(), oldVal);
					}
					if (instruct.getAssignVal().getType() == EQLObject.types.variable) {
						String leftVarName = instruct.getAssignName();
						String rightVarName = instruct.getParms().get(0);
						EQLObject rightVarExtra = instruct.getAssignVal();
						String rightVarRaw = rightVarExtra.toString().split("\\s+")[0];  // Assignments from cursor types only allow one value, parm #1 is index 0
	
						try {
							this.vars.put(leftVarName, this.pullCursorColumn(rightVarName, rightVarRaw));
							// Simple assignment copy value - cursors are copy reference
							this.debugMsg("engine", "Assignment of '" + instruct.getAssignName() + "' to: @" + rightVarName);
						} catch (SQLException | IOException | EQLException e) {
							this.errorMsg("engine", "Unable to assign value from cursor, error:" + e.getMessage());
							break;
						}
					}
					if (instruct.getAssignName().equals("eql_timeout_s")) {
						this.queryTimeout = instruct.getAssignVal().toInt();
						this.infoMsg("engine", "Query timeout for future statements is set to " + this.queryTimeout + " seconds.");
					}
					if (instruct.getAssignName().equals("eql_log_level")) {
						if (instruct.getAssignVal().getType() == EQLObject.types.integer) {
							this.logLevel = instruct.getAssignVal().toInt();
							this.infoMsg("engine", "Log level set to #" + instruct.getAssignVal());
						}
						else
							this.warnMsg("engine", "Internal variable eql_log_level must be an integer data type for use as a control variable at ln#" + instruct.getStartLine());;
					}
				} else if (instruct.getFunction().equals("connect")) {
					try {
						this.connect(instruct);
					} catch (EQLException e) {
						this.errorMsg("engine", "Unable to assign value from cursor, error:" + e.getMessage());
						break;
					}
				} else if (instruct.getFunction().equals("print")) {
					log.debug("Running print function");
					try {
						this.print(instruct);
					} catch (EQLException e) {
						this.errorMsg("engine", "Unable to assign value from cursor, error:" + e.getMessage());
						break;
					}
				} else {
					//Pass-through command to connection
					if (this.exec(instruct, true) == null)
						break;
				}
			} else {
				this.infoMsg("engine", "Skipping line #" + lineExecuting + ". Lead of line :(" + instruct.getAssignName().substring(0, 20).replace('\n', ' ') + "...)");
			}
		}
	}
	
	/**
	 * 
	 * @param varText - ?.column
	 * @param parmName - Parm name to pull cursor 
	 * @return
	 * @throws EQLException
	 * @throws SQLException
	 * @throws IOException
	 */
	public EQLObject pullCursorColumn(String parmName, String varText) throws EQLException, SQLException, IOException {
		//this.debugMsg("engine", "Type variable, var:" + leftVarName + " = var:" + rightVarName + ", val:" + rightVarExtra);
		String extraParts[] = varText.toString().split("\\.");
		EQLObject var = this.vars.get(parmName);
		
		if (var == null) {
			this.errorMsg("load var", "Unable to locate variable:" + parmName);
			return null;
		}
		if (var.getType() == EQLObject.types.cursor && extraParts.length == 2 && extraParts[0].equals("?")) {	// column reference requested for assignment - pull the requested column into the variable as a smart string
			String colName = extraParts[1];
			IASOnDemandDataSource ids = this.cursorWindowToJson("cursors." + parmName, 0, 10);	// pull 10 rows max
			EQLObject newVal = new EQLObject(this.getOneColumn(ids, colName));
			if (ids.getRows() >= 10)
				this.debugMsg("engine", "Loaded first 10 rows of col:" + colName + " from cursor: " + parmName);
			else
				this.debugMsg("engine", "Assigned " + ids.getRows() + " rows of col:" + colName + " from cursor: " + parmName);
			
			return newVal;
		} else {
			return this.vars.get(parmName);
		}
	}
	
	public String getOneColumn(IASOnDemandDataSource ids, String colName) throws EQLException {
		List<String> vals = new ArrayList<String>();
		for(Map<String, Object> row : ids.getData()) {	// pull the rows off the cursor
			if (!row.containsKey(colName)) {
				throw new EQLException("Requested column missing:" + colName);
			}
			
			Object oVal = row.get(colName);
			if (oVal != null && oVal instanceof String) {
				vals.add("'" + oVal.toString() + "'");
			} else {
				if (oVal == null) {
					log.debug("var:{} is null", colName);
					vals.add("");
				} else {
					log.debug("var:{} instance of:{}", colName, oVal.getClass());
					vals.add(oVal.toString());
				}
			}
		}
		return StringUtils.join(vals, ',');
	}
	
	/**
	 * Supports:
	 *   print @cursor;			print ?
	 *   print @cursor.mycol;	print ?.mycol
	 *   print @cursor.mycol @cursor.mycol2;	print ?.mycol ?.mycol2;
	 *   print @value;			print ?;
	 *   print @value1 @value2;	print ? ?;
	 *   print Some Text myval=@cursor.col;   print Some Text myval=?.col;   Prints: Some Text myval='sample value'
	 *   print Just some text;   print Just some text;
	 * @param inst
	 * @return
	 * @throws EQLException
	 */
	public boolean print(EQLInstruction inst) throws EQLException {
		// Scrape off the print and tokenize by space
		String text = inst.getAssignVal().toString();
		String refined = text.substring(text.toLowerCase().indexOf("print") + 5).trim();
		String[] parts = refined.split("\\s+");
		int pos = -1;
		
		StringBuilder sbLine = new StringBuilder();
		
		log.debug("Print found instruction has {} parts", parts.length);
		
//		if (parts.length == 0) {		// Empty line support 'Print;'
//			this.infoMsg("Print", "");
//		}
		
		for(String part : parts) {
			if (part.indexOf('?') < 0) {		// Segment does not have a variable
				sbLine.append(((sbLine.length() > 0) ? " " : "") + part);
			} else {							// Segment has a variable
				pos++;
				String parmName = inst.getParms().get(pos);
				EQLObject item = null;
				
				try {
					item = this.pullCursorColumn(parmName, part);
				} catch (SQLException | IOException e) {
					if (sbLine.length() > 0) {	// Drain any static text before throwing an error
						this.infoMsg("Print", sbLine.toString());
					}
					this.errorMsg("engine", "Unable to assign value from cursor, error:" + e.getMessage());
					return false;
				}

				if (item == null) {
					if (sbLine.length() > 0) {	// Drain any static text before print cursor entries
						this.infoMsg("Print", sbLine.toString());
					}
					this.errorMsg("print ln#" + inst.getStartLine(), "No value found for var:" + parmName);
					return false;
				} else if (item.getType() == EQLObject.types.cursor) {
					try {
						IASOnDemandDataSource ids = this.cursorWindowToJson("cursors." + parmName, 0, 10);
						String idList = StringUtils.join(ids.getColumnLabels(), ",");
						this.infoMsg("Print",  "- Cursor @" + parmName + " --------------");
						this.infoMsg("Print", idList);
						this.infoMsg("Print", new String(new char[idList.length()]).replace('\0',  '-'));
						int rownbr = 1;
						for(Map<String, Object> row : ids.getData()) {
							List<String> arow = new ArrayList<String>();
							for(String col : ids.getColumnNames()) {
								Object val = row.get(col);
								if (val != null)
									arow.add(val.toString());
								else
									arow.add("");	// null is empty string for now
							}
							// spacesToString((2 + parmName.length()) - (Integer.toString(rownbr).length() + 3)) + "r#" + rownbr + "=" + 
							this.infoMsg("Print", StringUtils.join(arow, ","));
							rownbr++;
						}
						this.infoMsg("Print", new String(new char[idList.length()]).replace('\0',  '-'));
						if (rownbr > 10) {
							this.infoMsg("Print", "@" + parmName + " print stops at " + 10 + " rows");
						}
					} catch (SQLException e) {
						this.errorMsg("Print", "@" + parmName + ":Unable to print cursor, SQL error:" + e.getMessage());
					} catch (IOException e) {
						this.errorMsg("Print", "@" + parmName + ":Unable to print cursor, IO error:" + e.getMessage());
					}
				} else {
					sbLine.append(((sbLine.length() > 0) ? " " : "") + item.toString());
					// this.infoMsg("var:" + parmName + " type:" + item.printType() + " ln#" + inst.getStartLine(), item.toString());
				}
			}
		}
		
		if (sbLine.length() > 0) {	// Drain any remaining static text before exiting
			this.infoMsg("Print", sbLine.toString());
		}

		return true;
	}

	
	protected boolean closeCursorIfLastReference(String currentName, EQLObject cursor) {
		try {
			if (cursor != null && cursor.getType() == EQLObject.types.cursor) {
				int refCnt = 0;
				for(String itemName:this.vars.keySet()) {
					EQLObject item = this.vars.get(itemName);
					if (item != null && item.getType() == EQLObject.types.cursor && item == cursor) {
						refCnt++;
					}
				}
				
				if (refCnt == 0) {	// Nothing left using this value so close it
					cursor.getCursor().close();
					cursor.getStatement().close();
					this.debugMsg("exec", "Closing previous cursor stored in '" + currentName + "'");
				}
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			this.errorMsg("closeCursor", "Error closing '" + currentName + "' error from driver:" + e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	public void closeConnection() {
		// Check for any open cursors/statements
		List<String>markedForDelete = new ArrayList<String>();
		for(String itemName:this.vars.keySet()) {
			EQLObject item = this.vars.get(itemName);
			try {
				if (item != null && item.getType() == EQLObject.types.cursor) {
					if (!item.getCursor().isClosed()) {
						item.getCursor().close();
					}
					if (!item.getStatement().isClosed()) {
						item.getStatement().close();
					}
					this.vars.put(itemName, null); // removal creates concurrent modification errors while running the for loop
					markedForDelete.add(itemName);
					this.debugMsg("close", "Closed cursor named '" + itemName + "'");
				}
			} catch (SQLException e) {
				this.errorMsg("closeConnection", "'" + itemName + "' Close error:" + e.getMessage());
				this.vars.put(itemName, null); // removal creates concurrent modification errors while running the for loop
			}
		}
		for(String key:markedForDelete) {
			this.vars.remove(key);
		}
		
		if (this.conn != null) {
			try {
				this.conn.close();
				this.infoMsg("close", "Closed database connection");
			} catch (SQLException e) {
				this.errorMsg("run", e.getMessage());
				e.printStackTrace();
			}
			this.conn = null;
		}
	}
	
	public void close() {
		this.closeConnection();
	}
	
	/**
	 * Single line execution logic implementation
	 * @param inst
	 * @param save
	 * @return
	 */
	public EQLObject exec(EQLInstruction inst, boolean save) {
		PreparedStatement stmt = null;
		try {
			if (conn == null || conn.isClosed()) {
				if (save)
					this.errorMsg("Execute", "No connection found. Use 'connect your_connection;' to establish a connection");
				
				return null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		try {
			//STEP 4: Execute a query
			log.debug("about to prepare statement");
			String statement = inst.getPreparedStmt(this,  vars);
			
			if (save)
				this.debugMsg(lineTitleToString(inst.getStartLine()), "Creating statement from: " + statement);
			
			// TODO: statement substitution before sending to the database
			log.debug("Preparing statement:{}", statement);
			
			Stopwatch timer = Stopwatch.createStarted();
			
			stmt = conn.prepareStatement(statement); //, ResultSet.TYPE_SCROLL_INSENSITIVE);
			if (this.queryTimeout > -1)
				stmt.setQueryTimeout(this.queryTimeout);
			
//			stmt = conn.prepareStatement(inst.getAssignVal().toString()); //, ResultSet.TYPE_SCROLL_INSENSITIVE);

			String currentVal = "";
			try {
				int idx = 1;
				int seq = 1;
				for (String val : inst.getParms()) {
					currentVal = val;
					EQLObject item = this.vars.get(val);
					if (item.getType() == EQLObject.types.string) {
						this.debugMsg(lineTitleToString(inst.getStartLine()), "Parm #" + seq + " named " + val + " set as string with value:" + item.toString());
						stmt.setString(idx, item.toString());
					} else if (item.getType() == EQLObject.types.rawText) {
						//this.debugMsg("exec", "Parm #" + idx + " named " + val + " ignored as already direct injected");
						idx--; // drop back one as this was already direct injected
					} else if (item.getType() == EQLObject.types.integer) {
						this.debugMsg(lineTitleToString(inst.getStartLine()), "Parm #" + seq + " named " + val + " set as integer with value:" + item.toString());
						stmt.setInt(idx, item.toInt());
					} else if (item.getType() == EQLObject.types.decimal) {
						this.debugMsg(lineTitleToString(inst.getStartLine()), "Parm #" + seq + " named " + val + " set as double with value:" + item.toString());
						stmt.setDouble(idx, item.toDouble());
					} else {
						this.debugMsg(lineTitleToString(inst.getStartLine()), "Parm #" + seq + " named " + val + " set as command string with value:" + item.toString());
						stmt.setString(idx, item.toString());
					}
					idx++;
					seq++;
				}
			} catch(SQLException se) {
				this.errorMsg(lineTitleToString(inst.getStartLine()), "SQL Exception processing variable '" + currentVal + "'. Error is :" + se.getMessage());
				se.printStackTrace();
				return null;
			} catch(NullPointerException se) {
				this.errorMsg(lineTitleToString(inst.getStartLine()), "Failed to locate query parameter named '" + currentVal + "'. The variable is missing or not defined. Make sure to escape the database command @ symbols with \\@ if sending to the database engine.");
				se.printStackTrace();
				return null;
			}

			ResultSet rs  = null;
			EQLObject ers = null;
			if (!stmt.execute()) {	// True means a result was obtained
				if (stmt.getUpdateCount() > -1)
					ers = new EQLObject("Statement affected " + stmt.getUpdateCount() + " rows, runtime:" + elapsedTimeToString(timer) + " (" + StringUtils.substring(statement, 0, 40) + ((statement.length() > 39) ? "...)" : ")"));
				else
					ers = new EQLObject("Statement completed successfully but did not return a result, runtime:" + elapsedTimeToString(timer) + " (" + StringUtils.substring(statement, 0, 40) + ((statement.length() > 39) ? "...)" : ")"));
				
				this.infoMsg(lineTitleToString(inst.getStartLine()), ers.toString());
			} else {
				rs = stmt.getResultSet();
				ers = new EQLObject(stmt, rs);
				if (inst.getAssignName() != null && inst.getAssignName().length() > 0) {
					EQLObject ers_old = this.vars.put(inst.getAssignName(), ers);		// Returns old value if replacement was performed
					this.closeCursorIfLastReference(inst.getAssignName(), ers_old);
					this.infoMsg(lineTitleToString(inst.getStartLine()), "Results ready (@" + inst.getAssignName() + ((save)? " and @eql_last_stmt" : "") + ") runtime:" + elapsedTimeToString(timer) + " (" + StringUtils.substring(statement, 0, 40) + ((statement.length() > 39) ? "...)" : ")"));
				} else if (save) {
					this.infoMsg(lineTitleToString(inst.getStartLine()), "Results ready (@eql_last_stmt) runtime:" + elapsedTimeToString(timer) + " (" + StringUtils.substring(statement, 0, 40) + ((statement.length() > 39) ? "...)" : ")"));
				}
				if (save) {
					EQLObject ers_old = this.vars.put("eql_last_stmt", ers);		// Returns old value if replacement was performed
					if (ers_old != null) {
						this.closeCursorIfLastReference("eql_last_stmt", ers_old);
					}
					this.debugMsg(lineTitleToString(inst.getStartLine()), "Executed query, results ready");
				}
			}
			return ers;
		} catch(SQLException | NullPointerException se) {
			this.errorMsg(lineTitleToString(inst.getStartLine()), se.getMessage());
			//Handle errors for JDBC
			se.printStackTrace();
			return null;
		} 
	}
	
	protected String lineTitleToString(int lineNbr) {
		String ln = Integer.toString(lineNbr);
		return "l#" + ln + spacesToString(5 - (2+ln.length()));
	}
	
	protected String spacesToString(int spaces) {
		if (spaces < 1) {
			return "";
		}
		
		return String.format("%1$" + spaces + "s", "");
	}
	
	protected String elapsedTimeToString(Stopwatch timer) {
		long t = timer.elapsed(TimeUnit.MILLISECONDS);
		double unit = (double)t;
		String txt = "ms";

		if (t > 3600000) {
			unit = unit / 3600000;
			txt = "h";
		} else if (t > 60000) {
			unit = unit / 60000.0;
			txt = "m";
		} else if (t > 1000) {
			unit = unit / 1000.0;
			txt = "s";
		}
		
		return String.format("%.1f", unit) + txt;
	}
	
	public IASOnDemandDataSource cursorWindowToJson(String name, int start, int end) throws EQLException, SQLException, IOException {
		IASOnDemandDataSource ldds = new IASOnDemandDataSource();
		String tableName = "";
		String libName = "";
		
		if (name.equalsIgnoreCase("__envVars")) {
			// TODO environment listing
		} else {
			if (name.contains(".")) {
				String[] parts = name.split("\\.");
				if (parts.length == 2) {
					libName = parts[0];
					tableName = parts[1];
				} else {
					throw new EQLException("Named object '" + name + "' needs to be in the format lib.table");
				}
			} else if (name.equalsIgnoreCase("__envLastSet")) {
				libName = "cursors";
				tableName = "eql_last_stmt";
			} else {
				throw new EQLException("(2)Named object '" + name + "' needs to be in the format lib.table");
			}
			
			if (!libName.equalsIgnoreCase("cursors")) {
				throw new EQLException("Named object '" + name + "' needs to be opened by a select statement");
			}
			
			EQLObject eCursor = this.vars.get(tableName);
			
			if (eCursor == null) {
				throw new EQLException("Cursor named '" + name + "' not found.");
			}
			if (eCursor.getType() != EQLObject.types.cursor) {
				throw new EQLException("Named object '" + name + "' is not a cursor type");
			}
			
			ResultSet cursor = eCursor.getCursor();

//			if (!cursor.absolute(start)) {
//				throw new EQLException("Request to moving cursor '" + name + "' to row #" + start + " pushed it beyond the result set boundary.");
//			}

			// If all rows are selected, grab a max of 1000 rows
			if (end == 0)
				end = 1000;
						
			if (eCursor.getRsmd() == null)
				eCursor.setRsmd(cursor.getMetaData());
			
			try {
				ldds.setName(tableName);
				List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>();
				int cnt = start - 1;
				
				//Get row from cache if size of cache shows loaded, when expired - continue to cursor.next logic
				if (eCursor.getCursor_cache().size() > start) {
					int pullCacheRows = eCursor.getCursor_cache().size();
					for(int idx=start; idx < Math.min(pullCacheRows, end); idx++) {
						rows.add(eCursor.getCursor_cache().get(idx));
						cnt++;
					}
				}
			
				if (cnt < (end - 1)) { // Are we fetching rows? make sure we get the best row sized transfered
					if ((end-start)>2)
						cursor.setFetchSize((end-start)+1);
					else
						cursor.setFetchSize(50);
				}
				
				boolean firstRow = false;
				if (eCursor.getCursor_cols().size() == 0)
					firstRow = true;
				
				log.debug("Counting vars for get data:  cnt:{} end:{}", cnt, end);
				while(cnt < (end - 1) && cursor != null && !cursor.isAfterLast() && cursor.next()) {
					Map<String,Object> row = new LinkedTreeMap<String,Object>();
					
					for(int idx=1; idx<=eCursor.getRsmd().getColumnCount(); idx++) {
						String lb = eCursor.getRsmd().getColumnLabel(idx);
						String nm = IASOnDemandDataSource.getSafeColumnName(eCursor.getRsmd().getColumnName(idx));
						if (firstRow) {
							eCursor.getCursor_cols().add(nm);
							eCursor.getCursor_labels().add(lb);
						}
						if (eCursor.getRsmd().getColumnType(idx) == Types.INTEGER)
							row.put(nm, cursor.getInt(idx));
						else if (eCursor.getRsmd().getColumnType(idx) == Types.DOUBLE)
							row.put(nm, cursor.getDouble(idx));
						else if (eCursor.getRsmd().getColumnType(idx) == Types.FLOAT)
							row.put(nm, cursor.getFloat(idx));
						else if (eCursor.getRsmd().getColumnType(idx) == Types.CHAR || eCursor.getRsmd().getColumnType(idx) == Types.VARCHAR)
							row.put(nm, cursor.getString(idx));
						else if (eCursor.getRsmd().getColumnType(idx) == Types.DATE)
							row.put(nm, cursor.getDate(idx).getTime());
						else
							row.put(nm, cursor.getString(idx));
					}
					firstRow = false;
					cnt++;
					rows.add(row);
					eCursor.getCursor_cache().add(row);
				}
				ldds.setData(rows);
				ldds.setColumnNames(eCursor.getCursor_cols());
				ldds.setColumnLabels(eCursor.getCursor_labels());
				int rowCount = Math.max(eCursor.getCursor_cache().size(), (cnt < (end-1))? 0 : end + 20);
				ldds.setRows(rowCount);		//Keep it paging if more records exist
				return ldds;
			} catch (Exception e) {
				e.printStackTrace();
				this.errorMsg("Reading cursor", e.getMessage());
			}			
		}
		
		return null;
	}
	
	public boolean connect(EQLInstruction inst) throws EQLException {
		String instLine = inst.getAssignVal().toString();
		String[] parts = instLine.split(" ");
		
		if (parts.length != 2) {
			throw new EQLException("Connect requires two words - syntax 'connect targetname;'. Command had " + parts.length + " words at ln#" + inst.getStartLine());
		}
		if (inst.getParms().size() > 1) {
			throw new EQLException("Too many parameters supplied for connect on ln#" + inst.getStartLine());
		}

		// Check for target and if it has a variable replacement instruction
		String connectTarget = parts[1];
		if (connectTarget.equals("?")) {
			//Parm only allows 1 at the moment - all other are syntax errors in execution
			if (inst.getParms().size() != 1) {
				throw new EQLException("Connection requested one parmater but none were supplied. Syntax 'connect @myvar;' expected on ln#" + inst.getStartLine());
			}
			String varName = inst.getParms().get(0);
			EQLObject val = vars.get(varName);
			if (val == null) {
				throw new EQLException("Variable '" + varName + "' not defined at time of use. ln#" + inst.getStartLine());
			}
			connectTarget = val.toString();
		}
		
		this.debugMsg("connect", "Connecting to " + connectTarget);

		// load connection settings
		Map<String,String> configs = this.getConfigItems(connectTarget);
		String className = configs.get("class");
		String jdbcUrl = configs.get("jdbc");
		String user = configs.get("user");
		String pass = configs.get("pass");

		if (className == null)
			throw new EQLException("Missing class entry in System Config database for '" + connectTarget + "'. Contact your site admin.");

		if (jdbcUrl == null) 
			throw new EQLException("Missing jdbc URL entry in System Config database for '" + connectTarget + "'. Contact your site admin.");

		this.closeConnection();		// Close any existing connections before making a new connection
		try {
			//Register JDBC driver
			Class.forName(className);
			this.debugMsg("connect", "JDBC driver located");
				
			//Make the connection
			if (user != null && user.length() > 0)
				conn = DriverManager.getConnection(jdbcUrl, user, pass);
			else
				conn = DriverManager.getConnection(jdbcUrl);

			this.infoMsg("connect", "Connection to '" + connectTarget + "' established");
		} catch(SQLException e) {
			this.errorMsg("connect", "Could not connect to '" + connectTarget + "'. Error from driver:" + e.getMessage());
			return false;
		} catch(Exception e){
			this.errorMsg("connect", "Could not locate the requested JDBC driver for '" + connectTarget + "'. Contact your site admin");
			e.printStackTrace();
			return false;
		} 
		return true;
	}
	
	protected Map<String,String> getConfigItems(String target) {
		Map<String,String> configItems = new HashMap<String,String>();
		
		try {
			Properties conf = this.config;
			configItems.put("class", conf.getProperty("eql."+target+".class"));
			configItems.put("jdbc", conf.getProperty("eql."+target+".jdbc"));
			configItems.put("user", conf.getProperty("eql."+target+".user", ""));
			configItems.put("pass", conf.getProperty("eql."+target+".pass", ""));
		} catch (Exception e) {
			this.errorMsg("getConfigItem", e.getMessage());
			e.printStackTrace();
		}
		
		return configItems;
	}

}
