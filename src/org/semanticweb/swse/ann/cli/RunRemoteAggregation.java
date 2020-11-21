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
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateConstants;
import org.semanticweb.swse.ann.agg.SlaveAnnAggregateArgs;
import org.semanticweb.swse.ann.agg.master.MasterAnnAggregator;
import org.semanticweb.swse.ann.agg.master.MasterAnnAggregatorArgs;

/**
 * Main method to conduct a distributed crawl using remote crawlers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteAggregation {
	private final static Logger _log = Logger.getLogger(RunRemoteAggregation.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote input (inferred) file, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzinO = new Option("gzin", "flag stating that remote input is gzipped");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option rawO = new Option("raw", "remote sorted (asserted) file");
		rawO.setArgs(1);
		rawO.setRequired(true);
		options.addOption(rawO);
		
		Option gzrawO = new Option("gzraw", "flag stating that remote input is gzipped");
		gzrawO.setArgs(0);
		options.addOption(gzrawO);
		
		Option linO = new Option("l", "local input file");
		linO.setArgs(1);
		linO.setRequired(true);
		options.addOption(linO);
		
		Option gzlO = new Option("gzl", "flag stating that local input is gzipped");
		gzlO.setArgs(0);
		options.addOption(gzlO);
		
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
		
		boolean gzIn = cmd.hasOption(gzinO.getOpt()), 
				gzlocal = cmd.hasOption(gzlO.getOpt()),
				gzraw = cmd.hasOption(gzrawO.getOpt());
		
		String in = cmd.getOptionValue(inO.getOpt());
		String raw = cmd.getOptionValue(rawO.getOpt());
		String local = cmd.getOptionValue(linO.getOpt());
		String out = cmd.getOptionValue(outO.getOpt());
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIAnnAggregateConstants.DEFAULT_RMI_PORT);
		
		runRemoteAggregation(in, gzIn, servers, raw, gzraw, local, gzlocal, out);
	}

	public static void runRemoteAggregation(String in, boolean gzin, RMIRegistries servers, String raw, boolean gzraw, String local, boolean gzlocal, String outdir) throws Exception {
		SlaveAnnAggregateArgs saa = new SlaveAnnAggregateArgs(in, raw, outdir);
		saa.setGzRaw(gzraw);
		saa.setGzIn(gzin);
		
		MasterAnnAggregatorArgs mra = new MasterAnnAggregatorArgs(local, RMIUtils.getLocalName(outdir), saa.getRemoteGatherDir(), saa);
		mra.setGzLocal(gzlocal);
		
		MasterAnnAggregator maa = new MasterAnnAggregator();
		maa.startRemoteTask(servers, RMIAnnAggregateConstants.DEFAULT_STUB_NAME, mra);
	}
}
