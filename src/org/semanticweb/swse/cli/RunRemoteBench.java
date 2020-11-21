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
import org.semanticweb.swse.bench.RMIBenchConstants;
import org.semanticweb.swse.bench.master.MasterBencher;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteBench {
	private final static Logger _log = Logger.getLogger(RunRemoteBench.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "input files, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzO = new Option("gzip", "remote input files are gzipped");
		gzO.setArgs(0);
		options.addOption(gzO);
		
		Option typeO = new Option("type", "0 for coordinate, 1 for gather/scatter");
		typeO.setArgs(1);
		typeO.setRequired(true);
		options.addOption(typeO);
		
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
		
		
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");
		
		boolean gz = cmd.hasOption("gzip");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIBenchConstants.DEFAULT_RMI_PORT);
		
		if(cmd.getOptionValue("type").equals("0")){
			runBenchCoordinate(in, gz, servers, out);
		} else if(cmd.getOptionValue("type").equals("1")){
			runBenchGatherScatter(in, gz, servers, out);
		} else{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
	}
	
	public static void runBenchCoordinate(String in, boolean gz, RMIRegistries servers, String outdir) throws Exception {
		long b4 = System.currentTimeMillis();
		MasterBencher mi = new MasterBencher(servers);
		mi.coordinate(in, gz, outdir);
		_log.info("Bench finished in "+(System.currentTimeMillis()-b4)+" ms.");
	}
	
	public static void runBenchGatherScatter(String in, boolean gz, RMIRegistries servers, String outdir) throws Exception {
		long b4 = System.currentTimeMillis();
		MasterBencher mi = new MasterBencher(servers);
		mi.gatherScatter(in, gz, outdir);
		_log.info("Bench finished in "+(System.currentTimeMillis()-b4)+" ms.");
	}
}
