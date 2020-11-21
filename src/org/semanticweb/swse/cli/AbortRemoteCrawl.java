package org.semanticweb.swse.cli;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.ldspider.RMICrawlerConstants;
import org.semanticweb.swse.ldspider.master.MasterCrawler;

/**
 * Main method to cleanly abort a remote crawl.
 * 
 * @author aidhog
 */
public class AbortRemoteCrawl {
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option helpO = new Option("h", "print help");
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
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
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMICrawlerConstants.DEFAULT_RMI_PORT);
		
		abortRemoteCrawl(servers);
	}
	
	
	public static void abortRemoteCrawl(RMIRegistries servers) throws Exception {
		MasterCrawler mc =  new MasterCrawler(servers);
		mc.abort();
	}
}
