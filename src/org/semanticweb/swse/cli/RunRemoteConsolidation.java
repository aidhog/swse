package org.semanticweb.swse.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.RMIConsolidationConstants;
import org.semanticweb.swse.cons.SlaveConsolidationArgs;
import org.semanticweb.swse.cons.master.MasterConsolidation;
import org.semanticweb.swse.cons.master.MasterConsolidationArgs;

/**
 * Main method to conduct distributed consolidation using remote servers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteConsolidation {
	private final static Logger _log = Logger.getLogger(RunRemoteConsolidation.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote input file, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzinO = new Option("gzin", "flag stating that input files are gzipped");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option ranksO = new Option("ranks", "optional local ranks nq.gz file");
		ranksO.setArgs(1);
		ranksO.setRequired(false);
		options.addOption(ranksO);
		
		Option gzranksO = new Option("gzranks", "flag stating that ranks file is gzipped");
		gzranksO.setArgs(0);
		options.addOption(gzranksO);
		
		Option outO = new Option("out", "remote/local output dir, can use a % delimiter");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option gzoutO = new Option("gzout", "flag stating that output files should be gzipped");
		gzoutO.setArgs(0);
		options.addOption(gzoutO);
		
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
		
		boolean gzin = cmd.hasOption("gzin");
		
		String ranks = cmd.getOptionValue("ranks");
		boolean gzranks = cmd.hasOption("gzranks");
		
		String in = cmd.getOptionValue("in");
		
		String out = cmd.getOptionValue("out");
		boolean gzout = cmd.hasOption("gzout");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIConsolidationConstants.DEFAULT_RMI_PORT);
		
		runRemoteConsolidation(in, gzin, servers, ranks, gzranks, out, gzout);
	}

	public static void runRemoteConsolidation(String in, boolean gzin, RMIRegistries servers, String out, boolean gzout) throws Exception {
		runRemoteConsolidation(in, gzin, servers, null, false, out, gzout);
	}
	
	public static void runRemoteConsolidation(String in, boolean gzin, RMIRegistries servers, String ranks, boolean gzranks, String out, boolean gzout) throws Exception {
		MasterConsolidation mc =  new MasterConsolidation();
		SlaveConsolidationArgs sca = new SlaveConsolidationArgs(in,
				SlaveConsolidationArgs.getDefaultSameasOut(out),
						SlaveConsolidationArgs.getDefaultOut(out, gzout)
										);
		sca.setGzIn(gzin);
		sca.setGzOut(gzout);
		
		MasterConsolidationArgs mca = new MasterConsolidationArgs(MasterConsolidationArgs.getDefaultSameasOut(out, gzout), sca);
		mca.setRanks(ranks, gzranks);
		mca.setGzSameAsOut(gzout);
		
		mc.startRemoteTask(servers, RMIConsolidationConstants.DEFAULT_STUB_NAME, mca);
	}
}
