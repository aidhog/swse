package org.semanticweb.swse.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.saor.RMIReasonerConstants;
import org.semanticweb.swse.saor.SlaveReasonerArgs;
import org.semanticweb.swse.saor.master.MasterReasoner;
import org.semanticweb.swse.saor.master.MasterReasonerArgs;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteReasoning {
	private final static Logger _log = Logger.getLogger(RunRemoteReasoning.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inTO = new Option("int", "remote input file to extract T-Box from, can use % delimiter");
		inTO.setArgs(1);
		inTO.setRequired(true);
		options.addOption(inTO);
		
		Option inAO = new Option("ina", "remote input file to consider as A-Box, can use % delimiter");
		inAO.setArgs(1);
		inAO.setRequired(true);
		options.addOption(inAO);
		
		Option gzinTO = new Option("gzint", "flag stating that input files are gzipped");
		gzinTO.setArgs(0);
		options.addOption(gzinTO);
		
		Option gzinAO = new Option("gzina", "flag stating that input files are gzipped");
		gzinAO.setArgs(0);
		options.addOption(gzinAO);
		
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
		
		boolean gzint = cmd.hasOption("gzint"), 
		        gzina = cmd.hasOption("gzina"), 
				gzrred = cmd.hasOption("gzrred");
		
		String inT = cmd.getOptionValue("int");
		String inA = cmd.getOptionValue("ina");
		String out = cmd.getOptionValue("out");
		String rredirs = cmd.getOptionValue("rredirs");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIReasonerConstants.DEFAULT_RMI_PORT);
		
		runRemoteReasoning(inT, gzint, inA, gzina, servers, rredirs, gzrred, out);
	}

	public static void runRemoteReasoning(String inT, boolean gzinT, String inA, boolean gzinA, RMIRegistries servers, String rr, boolean gzrred, String out) throws Exception {
		SlaveReasonerArgs sra = new SlaveReasonerArgs(inT, inA, rr, SlaveReasonerArgs.getDefaultTboxOut(out), SlaveReasonerArgs.getDefaultReasonedOut(out));
		sra.setGzInTbox(gzinT);
		sra.setGzInAbox(gzinA);
		sra.setGzRedirects(gzrred);
		
		MasterReasonerArgs mra = new MasterReasonerArgs(MasterReasonerArgs.getDefaultTboxOut(out), MasterReasonerArgs.getDefaultReasonedTboxOut(out), sra);
//		mra.setGzRedirects(gzlred);
		
		MasterReasoner mr =  new MasterReasoner();
		mr.startRemoteTask(servers, RMIReasonerConstants.DEFAULT_STUB_NAME, mra);
	}

	/**
	 * 
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws URISyntaxException 
	 */
	public static FileRedirects readRedirects(String redirs, boolean gzipped) throws IOException, ParseException, URISyntaxException {
		InputStream is = new FileInputStream(redirs);
		if(gzipped){
			is = new GZIPInputStream(is);
		}
		
		FileRedirects r = FileRedirects.createFileRedirects(is);
		_log.info("Added "+r.size()+" redirect pairs...");
		is.close();
		return r;
	}
}
