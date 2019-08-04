package com.ias.language.eql.imp;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EQLUtilities {
	private final static Logger log = LoggerFactory.getLogger(EQLUtilities.class.getCanonicalName());
	protected List<String> logs;
	protected Properties config;
	private SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss.SSS");
	protected int logLevel = 1;		// 1 debug, 2/info, 3/warn, 4/error

	public EQLUtilities(Properties config) {
		this.config = config;
		this.logs = new ArrayList<String>();
	}
	
	private String printCurrentDateTime() {
	    String strDate = sdf.format(new Date());
	    return strDate;
	}
	
	public void setLogLevel(int lvl) {
		this.logLevel = lvl;
	}
	
	public int getLogLevel() {
		return this.logLevel;
	}
	
	public void debugMsg(String src, String msg) {
		log.debug("{} - {}", src, msg);
		if (this.logLevel < 2)
			logs.add(String.format("%s [%s] DEBUG - %s", printCurrentDateTime(), src, msg));
	}

	public void infoMsg(String src, String msg) {
		log.info("{} - {}", src, msg);
		if (this.logLevel < 3)
			logs.add(String.format("%s [%s] INFO  - %s", printCurrentDateTime(), src, msg));
	}

	public void warnMsg(String src, String msg) {
		log.warn("{} - {}", src, msg);
		if (this.logLevel < 4)
			logs.add(String.format("%s [%s] WARN  - %s", printCurrentDateTime(), src, msg));
	}

	public void errorMsg(String src, String msg) {
		log.error("{} - {}", src, msg);
		if (this.logLevel < 5)
		logs.add(String.format("%s [%s] ERROR - %s", printCurrentDateTime(), src, msg));
	}

	public List<String> getLogs() {
		return logs;
	}
	
	public static String readSQLFile(String file) throws EQLException {
		log.debug("SQL read trying: {}", file);
		if (file == null || file.length() == 0 ) throw new EQLException("SQL Filename not supplied");
	    StringBuilder contentBuilder = new StringBuilder();
	    try (Stream<String> stream = Files.lines( Paths.get(file), StandardCharsets.UTF_8))
	    {
			log.debug("SQL File opened");
	        stream.forEach(s -> contentBuilder.append(s).append("\n"));
	    }
	    catch (IOException e)
	    {
			log.error(e.getLocalizedMessage());
			throw new EQLException("SQL file not found or unable to read error.");
	    }
	    return contentBuilder.toString();
	}

	public static Properties getPropsFile(String file) throws EQLException {
		
		if (file == null || file.length() == 0) return null;
		
		Properties config = new Properties();
		InputStream is;
		try {
			try {
				//Changed from user.dir to conf.dir due to new jvm class not found issues when moving user.dir down a folder to ./conf
				log.debug("Trying full path:{}", file);
				is = new BufferedInputStream(new FileInputStream(file));
				log.debug("Using supplied config file {}.", file);
			} catch (FileNotFoundException e) {
				log.warn("Unable to locate the config file {} using the current path, using the jar supplied entry.", file);
				throw new EQLException("Config file not found error.");
			}
			config.load(is);
			return config;
		} catch (IOException e) {
			log.error("Critical error reading {} file! Stack trace follows:{}", file, e.getMessage());
			e.printStackTrace();
			throw new EQLException("Failed to read config file, check stack trace for details.");
		}
	}

	public static Properties getPropsLocal(String file) {
		Properties config = new Properties();
		InputStream is;
		try {
			try {
				//Changed from user.dir to conf.dir due to new jvm class not found issues when moving user.dir down a folder to ./conf
				log.debug("Trying folder:{}", System.getProperty("conf.dir") + "/" + file);
				is = new BufferedInputStream(new FileInputStream(System.getProperty("conf.dir") + "/" + file));
				log.debug("Using the local config file {}.", file);
			} catch (FileNotFoundException e) {
				log.warn("Unable to locate the config file {} using the current path, using the jar supplied entry.", file);
				is = Class.class.getResourceAsStream("/resources/" + file); 
			}
			config.load(is);
		} catch (IOException e) {
			log.error("Critical error reading {} file! Stack trace follows:{}", file, e.getMessage());
			e.printStackTrace();
			return null;
		}
		return config;
	}
}
