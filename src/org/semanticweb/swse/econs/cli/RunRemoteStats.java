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
import org.semanticweb.swse.econs.stats.RMIEconsStatsConstants;
import org.semanticweb.swse.econs.stats.SlaveEconsStatsArgs;
import org.semanticweb.swse.econs.stats.master.MasterEconsStats;
import org.semanticweb.swse.econs.stats.master.MasterEconsStatsArgs;
import org.semanticweb.yars.nx.reorder.ReorderIterator;

/**
 * Main method to conduct a distributed crawl using remote crawlers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteStats {
	private final static Logger _log = Logger.getLogger(RunRemoteStats.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote input (inferred) file, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzinO = new Option("gzin", "flag stating that remote input is gzipped");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option sbO = new Option("sb", "flag to skip data build and just run statistics");
		sbO.setArgs(0);
		options.addOption(sbO);
		
		Option isaO = new Option("isa", "flag to ignore same-as triples in statistics");
		isaO.setArgs(0);
		options.addOption(isaO);
		
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
		
		Option oorderO = new Option("oo", "OPSC order flag (default 2103)... needed if tuples are >4");
		oorderO.setArgs(1);
		options.addOption(oorderO);
		
		Option sorderO = new Option("so", "SPOC order flag (default 0123)... needed if tuples are >4");
		sorderO.setArgs(1);
		options.addOption(sorderO);

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
		
		boolean gzIn = cmd.hasOption(gzinO.getOpt());
		boolean sb = cmd.hasOption(sbO.getOpt());
		boolean isA = cmd.hasOption(isaO.getOpt());
		
		String in = cmd.getOptionValue(inO.getOpt());
		String out = cmd.getOptionValue(outO.getOpt());
		
		int[] sorder = null;
		if(cmd.hasOption("so")){
			sorder = getMask(cmd.getOptionValue("so"));
		}
		
		int[] oorder = null;
		if(cmd.hasOption("oo")){
			oorder = getMask(cmd.getOptionValue("oo"));
		}
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIEconsStatsConstants.DEFAULT_RMI_PORT);
		
		runRemoteStats(in, gzIn, servers, out, sb, isA, sorder, oorder);
	}
	
	
	public static void runRemoteStats(String in, boolean gzin, RMIRegistries servers, String outdir) throws Exception {
		runRemoteStats(in, gzin, servers, outdir, false, false, null, null);
	}
	
	public static void runRemoteStats(String in, boolean gzin, RMIRegistries servers, String outdir, boolean skipBuild) throws Exception {
		runRemoteStats(in, gzin, servers, outdir, skipBuild, false, null, null);
	}

	public static void runRemoteStats(String in, boolean gzin, RMIRegistries servers, String outdir, boolean skipBuild, boolean ignoreSameas, int[] sorder, int[] oorder) throws Exception {
		SlaveEconsStatsArgs sesa = new SlaveEconsStatsArgs(in, outdir);
		sesa.setGzIn(gzin);
		
		MasterEconsStatsArgs mesa = new MasterEconsStatsArgs(outdir, sesa);
		mesa.setSkipBuild(skipBuild);
		mesa.setIgnoreSameAs(ignoreSameas);
		if(sorder!=null)
			mesa.setSpocOrder(sorder);
		if(oorder!=null)
			mesa.setOpscOrder(oorder);
		
		MasterEconsStats mes = new MasterEconsStats();
		mes.startRemoteTask(servers, RMIEconsStatsConstants.DEFAULT_STUB_NAME, mesa);
	}
	
	static int[] getMask(String arg){
		int[] reorder = new int[arg.length()];
		
		for(int i=0; i<reorder.length; i++){
			reorder[i] = Integer.parseInt(Character.toString(arg.charAt(i)));
		}
		
		return reorder;
	}
}
