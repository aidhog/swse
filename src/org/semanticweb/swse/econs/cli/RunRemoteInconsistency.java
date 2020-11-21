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
import org.semanticweb.swse.econs.incon.RMIEconsInconConstants;
import org.semanticweb.swse.econs.incon.SlaveEconsInconArgs;
import org.semanticweb.swse.econs.incon.master.MasterEconsIncon;
import org.semanticweb.swse.econs.incon.master.MasterEconsInconArgs;

/**
 * Main method to conduct distributed consolidation using remote servers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteInconsistency {
	private final static Logger _log = Logger.getLogger(RunRemoteInconsistency.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inSO = new Option("inS", "remote SPOC input file, can use % delimiter");
		inSO.setArgs(1);
		inSO.setRequired(true);
		options.addOption(inSO);
		
		Option gzinSO = new Option("gzinS", "flag stating that SPOC input files are gzipped");
		gzinSO.setArgs(0);
		options.addOption(gzinSO);
		
		Option inOO = new Option("inO", "remote OPSC input file, can use % delimiter");
		inOO.setArgs(1);
		inOO.setRequired(true);
		options.addOption(inOO);
		
		Option gzinOO = new Option("gzinO", "flag stating that input OPSC files are gzipped");
		gzinOO.setArgs(0);
		options.addOption(gzinOO);

		Option inTO = new Option("inT", "remote T-Box input file, can use % delimiter");
		inTO.setArgs(1);
		inTO.setRequired(true);
		options.addOption(inTO);
		
		Option gzinTO = new Option("gzinT", "flag stating that input T-Box files are gzipped");
		gzinTO.setArgs(0);
		options.addOption(gzinTO);
		
		Option inPO = new Option("inP", "remote pred stats file, can use % delimiter");
		inPO.setArgs(1);
		inPO.setRequired(true);
		options.addOption(inPO);
		
		Option gzinPO = new Option("gzinP", "flag stating that input pred stats files are gzipped");
		gzinPO.setArgs(0);
		options.addOption(gzinPO);

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
		boolean gzinT = cmd.hasOption("gzinT");
		boolean gzinP = cmd.hasOption("gzinP");
		
		String inS = cmd.getOptionValue("inS");
		String inO = cmd.getOptionValue("inO");
		String inT = cmd.getOptionValue("inT");
		String inP = cmd.getOptionValue("inP");

		
		String out = cmd.getOptionValue("out");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIEconsInconConstants.DEFAULT_RMI_PORT);
		
		runRemoteInconsistency(inS, gzinS, inO, gzinO, inT, gzinT, inP, gzinP, servers, out);
	}

	public static void runRemoteInconsistency(String inS, boolean gzinS, String inO, boolean gzinO, String inT, boolean gzinT, String inPS, boolean gzinPS, RMIRegistries servers, String out) throws Exception {
		SlaveEconsInconArgs seia = new SlaveEconsInconArgs(inS, inO, inT, inPS, out);
		seia.setGzInSpoc(gzinS);
		seia.setGzInOpsc(gzinO);
		seia.setGzTboxIn(gzinT);
		seia.setGzPredStatsIn(gzinPS);
		
		MasterEconsInconArgs meia = new MasterEconsInconArgs(seia);
		
		MasterEconsIncon mei = new MasterEconsIncon();
		mei.startRemoteTask(servers, RMIEconsInconConstants.DEFAULT_STUB_NAME, meia);
	}
	
}
