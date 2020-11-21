package org.semanticweb.swse.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.task.RMITaskMngrConstants;
import org.semanticweb.swse.task.master.MasterTaskMngr;
import org.semanticweb.swse.task.master.MasterTaskMngrArgs;
import org.semanticweb.swse.tasks.Tasks;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteSWSEBuild {
	private final static Logger _log = Logger.getLogger(RunRemoteSWSEBuild.class.getSimpleName());
	
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
		
		Option rredirO = new Option("rredirs", "remote redirects file, can contain %");
		rredirO.setArgs(1);
		rredirO.setRequired(true);
		options.addOption(rredirO);
		
		Option gzrredO = new Option("gzrred", "remote redirects files are gzipped");
		gzrredO.setArgs(0);
		gzrredO.setRequired(false);
		options.addOption(gzrredO);
		
		Option lredirO = new Option("lredirs", "local redirects file");
		lredirO.setArgs(1);
		lredirO.setRequired(true);
		options.addOption(lredirO);
		
		Option gzlredO = new Option("gzlred", "local redirects file is gzipped");
		gzlredO.setArgs(0);
		gzlredO.setRequired(false);
		options.addOption(gzlredO);
		
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
		
		boolean gzin = cmd.hasOption("gzin"), 
				gzrred = cmd.hasOption("gzrred"),
				gzlred = cmd.hasOption("gzlred");
		
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");
		String rredirs = cmd.getOptionValue("rredirs");
		String lredirs = cmd.getOptionValue("lredirs");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMITaskMngrConstants.DEFAULT_RMI_PORT);
		
		runRemoteSWSEBuild(in, gzin, servers, lredirs, gzlred, rredirs, gzrred, out);
	}

	public static void runRemoteSWSEBuild(String in, boolean gzin, RMIRegistries servers, String lr, boolean gzlred, String rr, boolean gzrred, String out) throws Exception {
		MasterArgs[] ma = Tasks.createTask(in, gzin, lr, gzlred, rr, gzrred, out);
		MasterTaskMngrArgs mtma = new MasterTaskMngrArgs(ma);
		
		MasterTaskMngr mr =  new MasterTaskMngr();
		mr.startRemoteTask(servers, RMITaskMngrConstants.DEFAULT_STUB_NAME, mtma);
	}
}
