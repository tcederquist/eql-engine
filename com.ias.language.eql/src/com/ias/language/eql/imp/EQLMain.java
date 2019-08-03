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

public class EQLMain {
	private final static Logger log = LoggerFactory.getLogger(EQLMain.class.getCanonicalName());

	// Call with parms
	// -Dconf.dir=path/to/conf
	// -f path/to/file
	// var="value" var2="value"
	
	public static void main(String[] args) {
		log.info("Starting main thread");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC")); // Enforce UTC for all jpa data movement - problem with jpa/mysql using local timezone
		log.info("Classpath:{}", System.getProperty("java.class.path"));

		Properties config = EQLUtilities.getPropsLocal("websvc.properties");
		
		log.info("test config:{}", config.getProperty("eqlLogLevel"));
		log.info("test config2:{}", config.getProperty("eql.ias.jdbc"));
		
		Options options = new Options();
		
		Option optionParm = Option.builder("p")
				.hasArgs()
				.desc("Default variable assignments")
				.valueSeparator(',')
				.argName("property=value")
				.optionalArg(true)
				.build();
				
		optionParm.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(optionParm);
		options.addOption("f", true, "SQL filename");
		options.addOption("r", false, "Restart Instruction line #");
		
		String[] a = new String[]{ "-p=val:1,val2:\"hello\",val3:'hi there!',val_4:Stuff", "-f=d:\\temp\\test.sql" };
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine line = parser.parse(options, a);
			String sqlFilename = line.getOptionValue("f");
			int restartLine = -1;
			String ln = line.getOptionValue('r');
			try {
				if (ln == null) {  // not provided
					restartLine = 0;
				} else {
					restartLine = Integer.parseInt(ln);
				}
			} catch (NumberFormatException e) {
				log.warn("Illegal value for -r, expecting a line #, recieved: {}", ln);
			}

			if (restartLine < 0) throw new EQLException("Illegal starting line, exiting commandline mode.");
			log.info("SQL file:{}", sqlFilename);
			log.info("Restart line:{}", restartLine);
			
			for(String item : line.getOptionValues("p")) {
				log.info("Parm:{}", item);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (args.length > 0 && args[0].equalsIgnoreCase("ondemand")) {
			EQLCommandLineDriver engine = new EQLCommandLineDriver(config);
			
//			engine.compile(code);
		}
	
		log.info("Exiting.");
	}

}
