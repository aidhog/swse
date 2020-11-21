package org.semanticweb.swse.ann.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.ann.incon.RMIAnnInconConstants;
import org.semanticweb.swse.ann.incon.SlaveAnnInconArgs;
import org.semanticweb.swse.ann.incons.master.MasterAnnIncon;
import org.semanticweb.swse.ann.incons.master.MasterAnnInconArgs;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteInconsistency {
	private final static Logger _log = Logger.getLogger(RunRemoteInconsistency.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote input file to extract inconsistencies from, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzinO = new Option("gzin", "flag stating that input files are gzipped");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option inTO = new Option("inT", "local tbox file");
		inTO.setArgs(1);
		inTO.setRequired(true);
		options.addOption(inTO);
		
		Option gzinTO = new Option("gzinT", "flag stating that input Tbox file is gzipped");
		gzinTO.setArgs(0);
		options.addOption(gzinTO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option outO = new Option("out", "remote/local output dir, can use a % delimiter");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
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
		
		boolean gzint = cmd.hasOption("gzinT"), 
		        gzin = cmd.hasOption("gzin"); 
		
		String inT = cmd.getOptionValue("inT");
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIAnnInconConstants.DEFAULT_RMI_PORT);
		
		runRemoteIncosistency(in, gzin, servers, inT, gzint, out);
	}

	public static void runRemoteIncosistency(String in, boolean gzin, RMIRegistries servers, String inT, boolean gzT, String out) throws Exception {
		SlaveAnnInconArgs sia = new SlaveAnnInconArgs(in, SlaveAnnInconArgs.getDefaultDataOut(out), SlaveAnnInconArgs.getDefaultInconsistenciesOut(out));
		sia.setGzIn(gzin);
		
		MasterAnnInconArgs mia = new MasterAnnInconArgs(inT, out, sia);
		mia.setGzTboxIn(gzT);
		
		MasterAnnIncon mr =  new MasterAnnIncon();
		mr.startRemoteTask(servers, RMIAnnInconConstants.DEFAULT_STUB_NAME, mia);
	}
}
