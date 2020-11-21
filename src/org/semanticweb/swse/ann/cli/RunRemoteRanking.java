package org.semanticweb.swse.ann.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.deri.idrank.RankGraph;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.RMIAnnRankingConstants;
import org.semanticweb.swse.ann.rank.SlaveAnnRankingArgs;
import org.semanticweb.swse.ann.rank.master.MasterAnnRanker;
import org.semanticweb.swse.ann.rank.master.MasterAnnRankingArgs;

/**
 * Main method to conduct a distributed crawl using remote crawlers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteRanking {
	private final static Logger _log = Logger.getLogger(RunRemoteRanking.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote input file, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option gzinO = new Option("gzin", "flag stating that remote input is gzipped");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option redirO = new Option("redirs", "remote *sorted* redirects file, can contain % delimiter");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option gzredO = new Option("gzred", "redirects file is gzipped");
		gzredO.setArgs(0);
		gzredO.setRequired(false);
		options.addOption(gzredO);
		
		Option itersO = new Option("iters", "number of pagerank iterations (int)");
		itersO.setArgs(1);
		itersO.setRequired(false);
		options.addOption(itersO);
		
		Option dO = new Option("d", "damping factor (float)");
		dO.setArgs(1);
		dO.setRequired(false);
		options.addOption(dO);
		
		Option tboxO = new Option("tbox", "include tbox links in ranking");
		tboxO.setArgs(0);
		tboxO.setRequired(false);
		options.addOption(tboxO);
		
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
				tbox = cmd.hasOption(tboxO.getOpt()),
				gzred = cmd.hasOption(gzredO.getOpt());
		int iters = RankGraph.ITERATIONS;
		double d = 0.85d;
		
		if(cmd.hasOption(dO.getOpt())){
			d = Float.parseFloat(cmd.getOptionValue("d"));
		}
		if(cmd.hasOption(itersO.getOpt())){
			iters = Integer.parseInt(cmd.getOptionValue("iters"));
		}
		
		String in = cmd.getOptionValue(inO.getOpt());
		String out = cmd.getOptionValue(outO.getOpt());
		String redirs = cmd.getOptionValue(redirO.getOpt());
		
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIAnnRankingConstants.DEFAULT_RMI_PORT);
		
		runRemoteRanking(in, gzIn, servers, redirs, gzred, iters, d, tbox, out);
	}

	public static void runRemoteRanking(String in, boolean gzin, RMIRegistries servers, String redirects, boolean gzred, int iters, double d, boolean tbox, String out) throws Exception {
		SlaveAnnRankingArgs sra = new SlaveAnnRankingArgs(in, redirects, out);
		
		sra.setGzRedirects(gzred);
		sra.setGzIn(gzin);
		sra.setTbox(tbox);
		
		
		MasterAnnRankingArgs mra = new MasterAnnRankingArgs(RMIUtils.getLocalName(out), sra);
		mra.setDamping(d);
		mra.setIterations(iters);
		
		MasterAnnRanker mar = new MasterAnnRanker();
		mar.startRemoteTask(servers, RMIAnnRankingConstants.DEFAULT_STUB_NAME, mra);
	}
}
