package org.semanticweb.swse.cli;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.swse.ldspider.remote.queue.OldDiskQueue;

public class DumpCrawlerQueue {
	static transient Logger _log = Logger.getLogger(DumpCrawlerQueue.class.getName());
	
	public final static String DIR = ".";
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws org.semanticweb.yars.nx.parser.ParseException, IOException {
		Options	options = new Options();
		
		Option qo = new Option("q", "queue directory");
		qo.setRequired(true);
		qo.setArgs(1);
		options.addOption(qo);
		
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		System.err.println("Dumping queue @"+cmd.getOptionValue("q"));
		
		OldDiskQueue.printQueue(cmd.getOptionValue("q"));
		System.err.flush();
	}
}
