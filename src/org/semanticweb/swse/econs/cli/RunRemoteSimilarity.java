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
import org.semanticweb.swse.econs.sim.RMIEconsSimConstants;
import org.semanticweb.swse.econs.sim.SlaveEconsSimArgs;
import org.semanticweb.swse.econs.sim.master.MasterEconsSim;
import org.semanticweb.swse.econs.sim.master.MasterEconsSimArgs;

/**
 * Main method to conduct distributed consolidation using remote servers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteSimilarity {
	private final static Logger _log = Logger.getLogger(RunRemoteSimilarity.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option psO = new Option("ps", "local pred stats filename (if skipping to aggregation)");
		psO.setArgs(1);
		options.addOption(psO);
		
		Option rdirO = new Option("rdir", "remote gather dir, can use % delimiter (if skipping to aggregation)");
		rdirO.setArgs(1);
		options.addOption(rdirO);
		
		Option inSO = new Option("inS", "remote SPOC input file, can use % delimiter");
		inSO.setArgs(1);
		options.addOption(inSO);
		
		Option gzinSO = new Option("gzinS", "flag stating that SPOC input files are gzipped");
		gzinSO.setArgs(0);
		options.addOption(gzinSO);
		
		Option inOO = new Option("inO", "remote OPSC input file, can use % delimiter");
		inOO.setArgs(1);
		options.addOption(inOO);
		
		Option gzinOO = new Option("gzinO", "flag stating that input OPSC files are gzipped");
		gzinOO.setArgs(0);
		options.addOption(gzinOO);

		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option outO = new Option("out", "remote output dir, can use a % delimiter");
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
		
		boolean gzinS = cmd.hasOption("gzinS");
		boolean gzinO = cmd.hasOption("gzinO");
		
		String inS = cmd.getOptionValue("inS");
		String inO = cmd.getOptionValue("inO");
		
		String out = cmd.getOptionValue("out");
		
		String rdir = cmd.getOptionValue("rdir");
		String ps = cmd.getOptionValue("ps");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIEconsSimConstants.DEFAULT_RMI_PORT);
		
		runRemoteSimilarity(inS, gzinS, inO, gzinO, servers, out, ps, rdir);
	}

	public static void runRemoteSimilarity(String inS, boolean gzinS, String inO, boolean gzinO, RMIRegistries servers, String out, String ps, String rdir) throws Exception {
		SlaveEconsSimArgs seia = new SlaveEconsSimArgs(inS, inO, out);
		seia.setGzInSpoc(gzinS);
		seia.setGzInOpsc(gzinO);
		
		MasterEconsSimArgs meia = new MasterEconsSimArgs(seia);
		
		if(ps!=null && rdir!=null){
			seia.setLocalGatherDir(rdir);
			meia.setPredicateStatsFilename(ps);
			meia.setSkipToAgg(true);
		}
		
		MasterEconsSim mei = new MasterEconsSim();
		mei.startRemoteTask(servers, RMIEconsSimConstants.DEFAULT_STUB_NAME, meia);
	}
	
}

