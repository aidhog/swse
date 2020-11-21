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
import org.semanticweb.swse.ann.repair.RMIAnnRepairConstants;
import org.semanticweb.swse.ann.repair.SlaveAnnRepairArgs;
import org.semanticweb.swse.ann.repair.master.MasterAnnRepair;
import org.semanticweb.swse.ann.repair.master.MasterAnnRepairArgs;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteRepair {
	private final static Logger _log = Logger.getLogger(RunRemoteRepair.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inTO = new Option("int", "remote input file to extract T-Box from, can use % delimiter");
		inTO.setArgs(1);
		inTO.setRequired(true);
		options.addOption(inTO);
		
		Option lintO = new Option("lint", "local input file to extract T-Box from");
		lintO.setArgs(1);
		lintO.setRequired(true);
		options.addOption(lintO);
		
		Option inRO = new Option("inr", "remote input file to consider as raw A-Box, can use % delimiter");
		inRO.setArgs(1);
		inRO.setRequired(true);
		options.addOption(inRO);
		
		Option inCO = new Option("inc", "remote input file to consider as closed A-Box, can use % delimiter");
		inCO.setArgs(1);
		inCO.setRequired(true);
		options.addOption(inCO);
		
		Option inconCO = new Option("incon", "local inconsistency file");
		inconCO.setArgs(1);
		inconCO.setRequired(true);
		options.addOption(inconCO);
		
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
		String inT = cmd.getOptionValue("int");
		String linT = cmd.getOptionValue("lint");
		String inR = cmd.getOptionValue("inr");
		String inC = cmd.getOptionValue("inc");
		String inconC = cmd.getOptionValue("incon");
		String out = cmd.getOptionValue("out");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIAnnRepairConstants.DEFAULT_RMI_PORT);
		
		runRemoteRepair(inT, linT, inR, inC, inconC, servers, out);
	}

	public static void runRemoteRepair(String inT, String linT, String inR, String inC, String inconC, RMIRegistries servers, String out) throws Exception {
		SlaveAnnRepairArgs sra = new SlaveAnnRepairArgs(inR, inC, inT, SlaveAnnRepairArgs.getDefaultRepairedOut(out));
		
		MasterAnnRepairArgs mra = new MasterAnnRepairArgs(linT, inconC, sra);
		
//		mra.setGzRedirects(gzlred);
		
		MasterAnnRepair mr =  new MasterAnnRepair();
		mr.startRemoteTask(servers, RMIAnnRepairConstants.DEFAULT_STUB_NAME, mra);
	}

}
