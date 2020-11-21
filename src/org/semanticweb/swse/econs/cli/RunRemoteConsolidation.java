package org.semanticweb.swse.econs.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.econs.ercons.RMIConsolidationConstants;
import org.semanticweb.swse.econs.ercons.SlaveConsolidationArgs;
import org.semanticweb.swse.econs.ercons.master.MasterConsolidation;
import org.semanticweb.swse.econs.ercons.master.MasterConsolidationArgs;

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
		
		Option reasonO = new Option("reason", "do some general reasoning for more complete consolidation");
		reasonO.setArgs(0);
		options.addOption(reasonO);

		Option sameasO = new Option("sa", "only extract sameas");
		sameasO.setArgs(0);
		options.addOption(sameasO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option redirO = new Option("redirs", "remote redirects file");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option gzredO = new Option("gzred", "redirects file is gzipped");
		gzredO.setArgs(0);
		gzredO.setRequired(false);
		options.addOption(gzredO);
		
//		Option ranksO = new Option("ranks", "remote ranks nq.gz file, possibly with % delimiter");
//		ranksO.setArgs(1);
//		ranksO.setRequired(false);
//		options.addOption(ranksO);
		
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
		
		boolean gzin = cmd.hasOption("gzin");
		boolean reason = cmd.hasOption("reason");
		boolean gzred = cmd.hasOption("gzred");
		boolean sao = cmd.hasOption("sa");
		
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");
		String redirs = cmd.getOptionValue("redirs");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIConsolidationConstants.DEFAULT_RMI_PORT);
		
		runRemoteConsolidation(in, gzin, redirs, gzred, reason, sao, servers, out);
	}

	public static void runRemoteConsolidation(String in, boolean gzin, String rredirs, boolean gzrredirs, boolean reason, boolean sameasOnly, RMIRegistries servers, String out) throws Exception {
		SlaveConsolidationArgs sca = new SlaveConsolidationArgs(in, rredirs, out);
		sca.setGzIn(gzin);
		sca.setGzRedirects(gzrredirs);
		
		MasterConsolidationArgs mca = new MasterConsolidationArgs(out, sca);
		mca.setReasonExtract(reason);
		mca.setSameasOnly(sameasOnly);
		
		MasterConsolidation mc = new MasterConsolidation();
		mc.startRemoteTask(servers, RMIConsolidationConstants.DEFAULT_STUB_NAME, mca);
	}
	
//	public static void runRemoteConsolidation(String in, boolean gzin, RMIRegistries servers, String ranks, String out) throws Exception {
//		MasterConsolidation mc =  new MasterConsolidation(servers);
//		mc.start(in, gzin, ranks, out);
//	}
}
