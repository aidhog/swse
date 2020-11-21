package org.semanticweb.swse.hobo.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsConstants;
import org.semanticweb.swse.hobo.stats.SlaveHoboStatsArgs;
import org.semanticweb.swse.hobo.stats.master.MasterHoboStats;
import org.semanticweb.swse.hobo.stats.master.MasterHoboStatsArgs;

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
		gzinO.setRequired(false);
		options.addOption(gzinO);
		
		Option aO = new Option("a", "remote access logs, can use % delimiter");
		aO.setArgs(1);
		aO.setRequired(true);
		options.addOption(aO);
		
		Option gzaO = new Option("gza", "flag stating that remote access logs are gzipped");
		gzaO.setArgs(0);
		gzaO.setRequired(false);
		options.addOption(gzaO);
		
		Option rO = new Option("r", "remote redirects file, can contain % delimiter");
		rO.setArgs(1);
		rO.setRequired(true);
		options.addOption(rO);
		
		Option gzrO = new Option("gzr", "redirects file is gzipped");
		gzrO.setArgs(0);
		gzrO.setRequired(false);
		options.addOption(gzrO);
		
		Option cO = new Option("c", "remote sorted contexts file, can contain % delimiter");
		cO.setArgs(1);
		cO.setRequired(true);
		options.addOption(cO);
		
		Option gzcO = new Option("gzc", "sorted contexts file is gzipped");
		gzcO.setArgs(0);
		gzcO.setRequired(false);
		options.addOption(gzcO);
		
		Option sbO = new Option("sb", "flag to skip data build and just run statistics");
		sbO.setArgs(0);
		options.addOption(sbO);
		
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
		
		boolean gzIn = cmd.hasOption(gzinO.getOpt());
		boolean gzA = cmd.hasOption(gzaO.getOpt());
		boolean gzR = cmd.hasOption(gzrO.getOpt());
		boolean gzC = cmd.hasOption(gzcO.getOpt());
		boolean sb = cmd.hasOption(sbO.getOpt());
		
		String in = cmd.getOptionValue(inO.getOpt());
		String a = cmd.getOptionValue(aO.getOpt());
		String r = cmd.getOptionValue(rO.getOpt());
		String c = cmd.getOptionValue(cO.getOpt());
		String out = cmd.getOptionValue(outO.getOpt());
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIHoboStatsConstants.DEFAULT_RMI_PORT);
		
		runRemoteStats(in, gzIn, a, gzA, r, gzR, c, gzC, servers, out, sb);
	}
	
	
	public static void runRemoteStats(String in, boolean gzin, String a, boolean gza, String r, boolean gzr, String c, boolean gzc, RMIRegistries servers, String outdir) throws Exception {
		runRemoteStats(in, gzin, a, gza, r, gzr, c, gzc, servers, outdir, false);
	}
	
	public static void runRemoteStats(String in, boolean gzin, String a, boolean gza, String r, boolean gzr, String c, boolean gzc, RMIRegistries servers, String outdir, boolean skipBuild) throws Exception {
		SlaveHoboStatsArgs sesa = new SlaveHoboStatsArgs(in, outdir);
		sesa.setGzIn(gzin);
		sesa.setGzA(gza);
		sesa.setGzR(gzr);
		sesa.setGzC(gzc);
		sesa.setA(a);
		sesa.setR(r);
		sesa.setC(c);
		
		MasterHoboStatsArgs mesa = new MasterHoboStatsArgs(outdir, sesa);
		mesa.setSkipBuild(skipBuild);
		
		MasterHoboStats mes = new MasterHoboStats();
		mes.startRemoteTask(servers, RMIHoboStatsConstants.DEFAULT_STUB_NAME, mesa);
	}
}
