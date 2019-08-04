package com.ias.language.eql.imp;

import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ias.language.objects.EQLObject;

public class EQLMain {
	private final static Logger log = LoggerFactory.getLogger(EQLMain.class.getCanonicalName());

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("Starting EQL Commandline mode");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC")); // Enforce UTC for all jpa data movement - problem with jpa/mysql using local timezone
		int returnCode = -1;
		
		System.out.println("");
		System.out.println("                __________    __ ");
		System.out.println("               / ____/ __ \\  / / ");
		System.out.println("              / __/ / / / / / /  ");
		System.out.println("             / /___/ /_/ / / /___");
		System.out.println("            /_____/\\___\\_\\/_____/");
		System.out.println("                 Extended SQL                ");
		System.out.println("          ______  __   _______   _____");
		System.out.println("         / __ ) \\/ /  /  _/   | / ___/");
		System.out.println("        / __  |\\  /   / // /| | \\__ \\");
		System.out.println("       / /_/ / / /  _/ // ___ |___/ /");
		System.out.println("      /_____/ /_/  /___/_/  |_/____/");
		System.out.println("   https://github.com/tcederquist/eql-engine");
		System.out.println("           Author: Tim Cederquist");
		System.out.println("            Apache License v2.0");
		System.out.println("");

		try {
			//////////// Read command line options
			//String[] a = new String[]{ "-p=eql_log_level:1,val:1,val2:\"hello\",val3:'hi there!',val_4:Stuff", "-f=d:\\test\\test.sql", "-c=d:\\test\\test.ini" };
			//String[] a = new String[]{ "-p=val:1,val2:'hello',val3:'hi there!',val_4:Stuff", "-f=d:\\test\\test.sql", "-c=d:\\test\\test.ini", "-r=0" };
			//String[] a = new String[]{ "-p=eql_log_level:1,mycon:ias,myval1:5,myval2:Hello,scope:AUTOMATIC,val:1,val2:hello,val3:hi there!,val_4:Stuff", "-f=d:\\test\\test.sql", "-c=d:\\test\\test.ini", "-r=8" };
			//String[] a = new String[]{ "-f=d:\\test\\test.sql" };

			CommandLine cmdParms = processArgs(args);
			String sqlFilename = cmdParms.getOptionValue("f");
			String cfgFilename = cmdParms.getOptionValue("c");
			boolean verbose = cmdParms.hasOption('v');
			int restartLine = getStartInstructionNumber(cmdParms);
			
			if (verbose) {
				log.info("Startup Classpath entries-----------");
				String[] cpEntries = System.getProperty("java.class.path").split(";");
				for (String line : cpEntries) {
					log.info("            : {}", line);
				}
			}
			
			log.info("cmdLine: SQL file    : {}", sqlFilename);
			log.info("cmdLine: CFG file    : {}", cfgFilename);
			log.info("cmdLine: Restart line: {}", restartLine);
			
			//Check for illegal values
			if (restartLine < 0) throw new EQLException("Illegal starting line, exiting commandline mode.");

			////////// Process config file
			Properties config = EQLUtilities.getPropsFile(cfgFilename);
//			if (config == null) config = EQLUtilities.getPropsLocal("websvc.properties");
//			log.info("Config : LogLevel    : {}", config.getProperty("eqlLogLevel"));

			/////////// Get SQL File
			String sql = EQLUtilities.readSQLFile(sqlFilename);
			log.info("SQL    : read chars  : {}", sql.length());
			
			/////////// Drive the engine
			if (sql != null && sql.length()> 1) {
				EQLCommandLineDriver engine = new EQLCommandLineDriver(config);
				engine.compile(sql);
				log.info("Compiled SQL stmts   : {}", engine.instructions.size());
				if (cmdParms.getOptionValues("p") != null) {
					for(String item : cmdParms.getOptionValues("p")) {
						String[] parts = item.split(":(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
						if (parts.length ==2) {
							log.info("Parm: key: {} = {}", parts[0], parts[1]);
							engine.setVariableDirect(parts[0], parts[1]);
						} else {
							throw new EQLException("Invalid value found in parm: " + item);
						}
					}
				}
				
				try {
					log.info("Starting execution--------------------");
					engine.run(restartLine);
					log.info("Execute completed---------------------");
					if (engine.getLastCompletedInstructionNumber() != engine.instructions.size()) {
						returnCode = 10;  // Didn't complete execution
					} else {
						returnCode = 0;
					}
				} finally {
					System.out.println("\n=====Start EQL engine log=======================================================\n");
					for(String msg : engine.getLogs()) {
						System.out.println(msg);
					}
					System.out.println("\n=====End EQL engine log=========================================================\n");
					log.info("Last instruction # completed:{}", engine.getLastCompletedInstructionNumber());
						if (returnCode != 0 || verbose) {
							StringBuilder cmdLnRestart = new StringBuilder();
							cmdLnRestart.append("-r=" + engine.getLastCompletedInstructionNumber() + " -p=");
							boolean separate = false;
							for(String key : engine.vars.keySet()) {
								if (separate) {
									cmdLnRestart.append(",");
								}
								EQLObject val = engine.vars.get(key);
								if (val.getType() != EQLObject.types.cursor) {
									cmdLnRestart.append(key + ":" + val);
									log.debug("Var: {}  Type:{}  Val:{}", key, val.getType(), val);
									separate = true;
								}
							}
							log.info("Restart parms list: {}", cmdLnRestart.toString());
						}
				}
			}
		} catch (EQLException e) {
			log.error("Exiting with failure: {}", e.getLocalizedMessage());
			if (returnCode == 0) returnCode = 10;
		}
	
		//Clean-up and exit
		log.info("Exit code:{}", returnCode);
		if (returnCode != 0) System.exit(returnCode);
	}

	static CommandLine processArgs(String[] args) {
		//Read command line options
		// Call with parms
		// -Dconf.dir=path/to/conf
		// -f path/to/sql
		// -c path/to/config
		// var="value" var2="value"
		Options options = new Options();
		options.addOption("f", true, "SQL filename");
		options.addOption("r", true, "Restart Instruction line #");
		options.addOption("c", true, "Configuration filename");
		
		options.addOption(
				Option.builder("v")
				.hasArg(false)
				.desc("Verbose output")
				.build()
				);
		
		options.addOption(
				Option.builder("p")
				.hasArgs()
				.desc("Default variable assignments")
				.valueSeparator(',')
				.argName("property=value")
				.optionalArg(true)
				.build()
				);
		
		CommandLineParser parser = new DefaultParser();
		try {
			return parser.parse(options, args);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.error(e.getLocalizedMessage());
		}
		return null;
	}
	
	static int getStartInstructionNumber(CommandLine cmdLine) {
		String ln = cmdLine.getOptionValue('r');
		try {
			if (ln == null) {  // not provided
				return 0;
			} else {
				return Integer.parseInt(ln);
			}
		} catch (NumberFormatException e) {
			log.warn("Illegal value for -r, expecting a line #, recieved: {}", ln);
		}
		return -1;
	}
}
