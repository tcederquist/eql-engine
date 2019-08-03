package com.ias.language.objects;

import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ias.language.eql.imp.EQLException;
import com.ias.language.eql.imp.EQLUtilities;

/**
 * Manages an EQL instruction element
 * @author tim_c
 *
 */
public class EQLInstruction {
	private final static Logger log = LoggerFactory.getLogger(EQLInstruction.class.getCanonicalName());

	protected int rawChars;		// Length of characters in raw input including comment length
	protected int rawLines;		// Number of lines this segment used in raw input
	protected int rawLeadLines;	// Number of newlines until first instruction
	protected int startLine;	// Line number in the raw input that starts this instruction line
	//protected String instructLine;	// Line without comments
	protected String rawInstLine;	// original string as assigned (trimmed of leading and trailing spaces)
	
	protected String assignName;	// Variable name if exists
	protected EQLObject assignVal;		// Variable value is assigned - if no variable this still contains the eqlobject version of the string
	protected String function;		// function - first command if not assignment
//	protected boolean isPassThrough;	// To pass-through to the sql engine
	protected List<String> parms;		// List of parms as found in statements
	
	public EQLInstruction() {
		rawChars = 0;
		rawLines = 0;
//		isPassThrough = false;
		function = null;
		assignName = null;
		parms = new ArrayList<String>();	// Maintains insertion order which is significant for parm substitution, duplicates must be acceptable
	}
	
	public String getAssignName() {
		return assignName;
	}
	
	public String getFunction() {
		return function;
	}

	public List<String> getParms() {
		return parms;
	}
	
	private int smallestPositive(int a, int b) {
		if (a < 0)
			return b;
		else if (b < 0)
			return a;
		return Math.min(a, b);
	}
	
	public boolean compile() throws EQLException {
		// Check if @ assignment
		int pos = 0;
		if (this.rawInstLine.startsWith("@")) {
			pos = this.rawInstLine.indexOf('=');
			if (pos > 0) {
				this.function = "var";
				this.assignName = this.rawInstLine.substring(1, pos).trim().toLowerCase();
				log.debug("Assignment type instruction of var:{} index of =:{}", this.assignName, pos);
			} else {
				throw new EQLException("Illegal instruction starting with @ but expected an '=' assignment character that was not found. line #" + this.startLine);
			}
		}
		
		// Check for variable assignment entries
		String[] strtok = this.rawInstLine.split(" ");
		String instructLine = this.rawInstLine;
		if (strtok.length > 0) {
			//Has a eql function been identified - if not then assign the first word as the pass-through function
			if (this.function == null || this.function.length() == 0) {
				this.function = strtok[0].toLowerCase();
				log.debug("Function located:{}", this.function);
			}

			//Parameter list identification logic - keep case to allow matching logic to follow
			int varLoc = instructLine.indexOf('@', 1);
			int limit = 1000;
			while(varLoc > 0 && limit > 0) {
				int nxt = -1;
				boolean atDlm = false;
				
				// Find the closest end of line or end of instruction marker
				nxt = smallestPositive(nxt, instructLine.indexOf(' ', varLoc)); // space and ; are expected delimiters
				nxt = smallestPositive(nxt, instructLine.indexOf('=', varLoc)); // if used on the left side of an equation w/o space 
				nxt = smallestPositive(nxt, instructLine.indexOf(',', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('(', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf(')', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('*', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('-', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('/', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('+', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('.', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf('\'', varLoc)); // if used in a query operational clause w/o space
				nxt = smallestPositive(nxt, instructLine.indexOf(';', varLoc));
				nxt = smallestPositive(nxt, instructLine.indexOf('\n', varLoc));
				nxt = smallestPositive(nxt, instructLine.indexOf('\r', varLoc));

				if (smallestPositive(nxt, instructLine.indexOf('@', varLoc + 1)) != nxt) {	// need to make sure we check beyond the @ at the current position!
					atDlm = true;
					nxt = smallestPositive(nxt, instructLine.indexOf('@', varLoc + 1)); // using @ can delimit values inside more complex statements table abced_@myvar_1@_55 
				}

				if (nxt == -1)	// End of line found instead of dlm
					nxt = instructLine.length();

				log.trace("Next closest eol or instruction:{}", nxt);
				
				String var = instructLine.substring(varLoc + 1, nxt);
				this.parms.add(var);
				
				log.trace("Added parm entry: {}", var);
				
				// For each parm replace with ? for later jdbc param substitution - case sensitive at first and then all names changed to lowercase to make case-insensitive for easy use
					// If var was bounded with syntax @var@ then consume the next @ to allow normal advancing logic. Legal for 2 side by side vars is @myvar2@@myvar2@More_text
				log.trace("Before instructline replace @:{}", instructLine);
				instructLine = instructLine.replaceFirst("@" + var + ((atDlm)? "@": ""), "?");
				log.trace("After instructline replace @:{}", instructLine);
				varLoc = instructLine.indexOf('@', 1);
				limit--;
			}
			
			//Switch to lower case for insensitive match logic rule
			for (int idx=0; idx < this.parms.size(); idx++) {
				this.parms.set(idx, this.parms.get(idx).toLowerCase());
			}
		} else {
			throw new EQLException("Illegal instruction '" + this.rawInstLine + "', no command found or parse failed at line #" + this.startLine);
		}
		
		// Put back the escaped values
		instructLine = instructLine.replaceAll("~~Q", "'");
		instructLine = instructLine.replaceAll("~~B", "\\");
		instructLine = instructLine.replaceAll("~~D", "\"");
		instructLine = instructLine.replaceAll("~~A", "@");
		instructLine = instructLine.replaceAll("~~S", ";");

		if (this.function.equals("var")) {
			log.debug("Instruction line:{} :: pos:{}", instructLine, pos);
			pos = instructLine.indexOf('=');	// Refresh the value incase of assignment changes like var substitution above
			this.assignVal = new EQLObject(instructLine.substring(pos + 1, instructLine.length()).trim());
			
			if (this.assignVal.type == EQLObject.types.statement) {
				strtok = instructLine.split(" ");
				// As a statement object - override the var and make a statement - having a varname will signal the engine to treat it as a cursor assignment 
				if (strtok.length > 0) {
					this.function = strtok[0];
				}
			}
		} else {
			this.assignVal = new EQLObject(instructLine);
		}

		return true;
	}
	
	/*
	 * Compiles the statement using string substitutions for any rawText values directly injected into the SQL statement
	 */
	public String getPreparedStmt(EQLUtilities utility, Map<String,EQLObject> vars) {
		if (this.assignVal.getType() == EQLObject.types.statement) {
			String statement = this.assignVal.toString();
			log.debug("Starting statement:{}", statement);
			int qmarkfound = 0; // number of marks found to reduce the index as they are replaced
			for (int idx=0; idx < this.parms.size(); idx++) {
				String pName = this.parms.get(idx);
				EQLObject item = vars.get(pName);
				if (item != null && item.getType() == EQLObject.types.rawText) {
					log.debug("Parm #{} name:{} rawtext:{}", idx, pName, item.toString());
					int qmarkPos = 0;	// character position moving through the string
					for(int cnt=0; cnt <= idx - qmarkfound; cnt++) {
						qmarkPos = statement.indexOf('?', qmarkPos + 1);
					}
					if (qmarkPos > 0) {
						utility.debugMsg("exec", "Parm #" + (idx + 1) + " named " + pName + " direct injected to statement with value:" + item.toString());
						statement = statement.substring(0, qmarkPos) + item.toString() + statement.substring(qmarkPos + 1);
						qmarkfound++;
					} else {
						log.error("Did not find the #:{} statement question mark to direct inject name:{}", idx, pName);
					}
				} else {
					if (item == null)
						log.error("Parameter didn't return a value by name:{}", pName);
					else
						log.debug("Parm #{} name:{} is not a rawtext type", idx, pName);
				}
			}
			return statement;
		} else {
			log.error("Internal error: Called prepare statement on a non-statement string: {}", this.assignVal.toString());
		}
		return null;
	}
	
	private static EQLInstruction extractRawLine(CharArrayReader car, int startLine) throws EQLException, IOException {
		boolean inQ = false;
		boolean inEsc = false;
		boolean inDashComment = false;	// --
		boolean inDashCommentOne = false;
		boolean inPartOneComment = false;	// /* first part
		boolean inPartOneUncomment = false;
		boolean inMultiLineComment = false;	// /* comment style
		boolean commandStarted = false;
		boolean inMultiLinePartialTest = false;
		boolean endFound = false;
		
		char inQchar = ' ';
		int pos = 0;
		int lines = 0;
		int leadLines = 0;
		
		StringBuilder line = new StringBuilder();
		int data = car.read();
		while (data != -1) {
			pos++;
			char c = (char) data;
			if (c == '\n') {
				lines++;
				if (!commandStarted)
					leadLines++;
			}
			if (inMultiLinePartialTest && !inQ && !inEsc && !inMultiLineComment && c =='/') {
				throw new EQLException("Nested /* */ found but not supported on line #" + (startLine + lines));
			} else {
				inMultiLinePartialTest = false;
			}
			
			// Add special comment characters back into the stream if the second part didnt become the multi-part comment
			if (inDashCommentOne && c != '-') {
				inDashCommentOne = false;
				line.append('-');	// Put the - back into the char stream
			} else if (inPartOneComment == true && c != '*') {
				inPartOneComment = false;
				line.append('/');
			}

			// In comment is highest priority statement
			if (inDashComment || inMultiLineComment) {
				if (inDashComment && (c == '\n' || c == '\r')) {
					inDashComment = false;
				} else if (inMultiLineComment && inPartOneUncomment && (c == '/')) {
					inPartOneUncomment = false;
					inMultiLineComment = false;
				} else if (inMultiLineComment && (c == '*')) {
					inPartOneUncomment = true;
				} else {
					inPartOneUncomment = false;
				}
			} else if (inEsc) {	// In escape is second priority
				inEsc = false;	// Only lasts one character
				switch (c) {
					case '\'':
						line.append("~~Q");
						break;
					case '\\':
						line.append("~~B");
						break;
					case '"':
						line.append("~~D");
						break;
					case '@':
						line.append("~~A");
						break;
					case ';':
						line.append("~~S");
						break;
					default:
						throw new EQLException("Illegal escape character sequence \\" + c + " at character :" + pos);
				}
			} else if (inQ) {	// In quote is third priority
				line.append(c);
				if (c == inQchar) {
					// End of quote
					inQ = false;
				}
			} else {
				switch (c) {
					case '\r':
						break;
					case '\n':
						//line.append(" ");	// for newline where next character is first on next line - space makes sure the dlm for sql is valid
						line.append(c);
						break;
					case '-':
						if (inDashCommentOne) {
							inDashComment = true;
							inDashCommentOne = false;
						}
						else {
							inDashCommentOne = true;
						}
						break;
					case '\\':
						inEsc = true;
						break;
					case '\'':
					case '"':
						inQ = true;
						inQchar = c;
						line.append(c);
						break;
					case '/':
						inPartOneComment = true;
						break;
					case '*':
						if (inPartOneComment) {
							inMultiLineComment = true;
						} else {
							line.append(c);
							inMultiLinePartialTest = true;
						}
						inPartOneComment = false;
						break;
					case ';':
						endFound = true;
						break;
					default:
						line.append(c);
						if (!Character.isWhitespace(c))
							commandStarted = true;
						break;
				}
			}
			
			if (endFound)
				break;
			data = car.read();
		}
		
		EQLInstruction i = new EQLInstruction();
		i.rawInstLine = line.toString().trim();
		//i.instructLine = line.toString().trim();
		i.rawChars = pos;
		i.rawLines = lines;
		i.rawLeadLines = leadLines;
		return i;
	}

	public static List<EQLInstruction> InstructionFactory(String segment) throws EQLException, IOException {
		List<EQLInstruction> retInst = new ArrayList<EQLInstruction>();
		char ca[] = segment.toCharArray();
		CharArrayReader car = new CharArrayReader(ca);
		int lineNbr = 1;
		EQLInstruction nextLine = extractRawLine(car, lineNbr);
		while (processInstruction(nextLine, lineNbr)) {
			retInst.add(nextLine);
			nextLine = extractRawLine(car, lineNbr);
		}

		return retInst;
	}

	public static EQLInstruction SingleInstructionFactory(String segment) throws EQLException, IOException {
		char ca[] = segment.toCharArray();
		CharArrayReader car = new CharArrayReader(ca);
		int lineNbr = 1;
		EQLInstruction nextLine = extractRawLine(car, lineNbr);
		if (processInstruction(nextLine, lineNbr))
			return nextLine;
		else
			return null;
	}
	
	private static boolean processInstruction(EQLInstruction inst, int lineNbr) {
		if(inst != null && inst.rawInstLine.length() > 0) {
			inst.startLine = lineNbr + inst.rawLeadLines;
			lineNbr += inst.rawLines;
			try {
				inst.compile();
				log.debug("Line found: {}", inst.getAssignVal().toString());
				log.debug("\tStarting line #{} function:{} name:{} value:{}", inst.startLine, inst.function, inst.assignName, inst.assignVal);
				for(String var : inst.parms) {
					log.debug("\t\tParm Found:{}", var);
				}
				return true;
			} catch (EQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	public int getStartLine() {
		return startLine;
	}

	public EQLObject getAssignVal() {
		return assignVal;
	}
}

/*		while ((marker = nextToken(ca, start)) > 0) {
String token = segment.substring(start, marker).trim();
char c = token.charAt(0);
if (c == '=' || c == '.')
	lastAction = c;
else {
	if (Character.isLetter(c)) {
		// Word type
		if (lastAction == '=') {
			i.assignName = lastWord;
			lastWord = token;
		} else if (lastAction == '.') {
			i.objTarget = lastWord;
			
		}
			
	} else if (Character.isDigit(c)) {
		// Numeric type
	} else if (c == '-') {
		// Numeric type
	} else if (c == '.') {
		// Object Type
	} else if (c == '{' || c == '\'' || c == '"') {
		// String type
		i.parms.add(new EQLString(segment.substring(start, pos)));		// simply add any string to parm stack
		if (!oSeg && !fSeg) {		// Default to format of declare.string({})
			oSeg = true;
			i.objTarget = "declare";
			i.function = "string";
		}
	}
}
start = marker;
}
*/
/*		int remIdx = line.indexOf('#');
int startIdx = line.indexOf("(");
int lastIdx = line.lastIndexOf(")");
int lastEnd = line.lastIndexOf(";");

if (startIdx > lastIdx || startIdx < 3) {
i.compileRC++;
ens.error("onDemand instruction compilation", "Mismatched parenthetics, expecting function(); format");
}
if (lastEnd < lastIdx) {
i.compileRC++;
ens.error("onDemand instruction compilation", "Statement did not end with ';' expecting function(); format");
}
if (remIdx >= 0 && remIdx < startIdx) {
ens.debug("onDemand instruction compilation", "Statement is commented out:" + line);
return null;
}

if (i.compileRC == 0) {
i.function = line.substring(0, startIdx).trim();

if (i.function.length() > 0) {
	boolean inSQuote = false;
	boolean inDQuote = false;
	boolean inName = true;
	boolean inEscape = false;
	StringBuilder name = new StringBuilder();
	StringBuilder value = new StringBuilder();
	String interior = line.substring(startIdx + 1, lastIdx);
	
	for (int idx = 0; idx < interior.length(); idx++) {
		char c = interior.charAt(idx);
		boolean store = true;
		
		switch(c) {
			case '\"':
				if (!inSQuote && !inEscape) {
					inDQuote = !inDQuote;
					store = false;
				}
				break;
			case '\'':
				if (!inDQuote && !inEscape) {
					inSQuote = !inSQuote;
					store = false;
				}
				break;
			case '\\':
				if (!inEscape)
					store = false;
				
				inEscape = !inEscape;
				break;
			case '=':
				if (!inDQuote && !inSQuote && !inEscape) {
					store = false;
					if (inName)
						inName = false;
					else {
						i.compileRC++;
						ens.error("onDemand instruction compilation", "Equale found after value, use quotes if part of a string. Name=Value expected, found Name=Value= format");
					}
				}
				break;
			case ',':
				if (!inDQuote && !inSQuote && !inEscape) {
					i.nvPairs.put(name.toString().trim(), value.toString().trim());
					store=false;
					name = new StringBuilder();
					value = new StringBuilder();
					inName = true;
				}
				break;
			default:
				break;
		}
		
		if (store) {
			if (inName)
				name.append(c);
			else
				value.append(c);
		}
	}
	if (inDQuote || inSQuote) {
		i.compileRC++;
		ens.error("onDemand instruction compilation", "Mismatched single or double quotes in expression");
	}
	if (inEscape) {
		i.compileRC++;
		ens.error("onDemand instruction compilation", "Illegal escape character '/' left at the end of a string");
	}
	
	i.nvPairs.put(name.toString().trim(), value.toString().trim());
} else {
	// No instruction found - typically an empty line
	return null;
}
}
*/

 	/**
	 * Extract from boundary to boundary
	 * 	String boundary {}, ', "
	 *  Word boundary is any non Alpha/Numeric
	 *  number boundary is any non numeric or . or -
	 *  Starts with # its a number
	 *  Starts with Alpha its a word
	 *  Starts with {}, ', " its a string
	 *  Starts with . it returns directly
	 *  Eat space and crlf until first printable character found
	 *  Returns all characters, caller must trim - allows caller to advance string the correct length 
	 * @param segment
	 * @param start
	 * @return
	 * @throws EQLException 
	 */
 /*
	private static int nextToken(char[] ca, int start) throws EQLException {
		boolean starting = true;
		boolean isWord = false;
		boolean isNum = false;
		boolean inQ = false;
		boolean inEsc = false;
		char inQchar = ' ';
		int pos = start;
		for(char c : ca) {
			if (starting && !Character.isWhitespace(c)) {	// Something to start with found?
				if (Character.isLetter(c)) {
					isWord = true;
				} else if (Character.isDigit(c)) {
					isNum = true;
				} else {
					switch (c) {
						case '\'':
						case '"':
						case '{':
							inQ = true;
							isWord = true;
							inQchar = c;
							break;
						case '}':
							throw new EQLException("Illegal use of } at " + start + pos);
						default:
							throw new EQLException("Illegal character '" + c + "' at " + start + pos);
					}
				}
				starting = false;
			}
				
			if (!starting) {						// Inside a word or number
				if (inQ) {								// In a quote - anything goes until a non escaped close quote is found
					if (inEsc)
						inEsc = false;
					else {
						switch (c) {
							case '{':
								throw new EQLException("Illegal nested { inside a quote, escape with \\ at position:" + start + pos);
							case '}':
							case '\'':
							case '"':
								if (inQchar == c) {		// Only consider symmetrical string boundaries
									inQ = false;
									return pos;
								}
								break;
							case '\\':
								inEsc = true;
						}
					}
				} else {								// Not in quote
					if (Character.isWhitespace(c))
						return pos;
					if (Character.isDigit(c) || Character.isLetter(c)) {
						
					} else {
						switch (c) {
							case '.':
								if (isWord)
									return pos;
								
								if (ca.length > pos) {	// Check if object notation with alpha after . or if numeric then consider part of the number
									if (!Character.isDigit(ca[pos+1])) {
										return pos - 1;
									}
								}
							case ',':
							case '(':
							case '{':
								return pos;
							case ')':
							case '}':
								throw new EQLException("Illegal use of } at " + start + pos);
						}
					}
				}
			}
			pos++;
		}
		if (pos - start == 0)
			return 0;
		
		return pos;
	}*/
