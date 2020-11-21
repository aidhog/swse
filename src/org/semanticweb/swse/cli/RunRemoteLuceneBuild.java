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
import org.semanticweb.swse.lucene.RMILuceneConstants;
import org.semanticweb.swse.lucene.SlaveLuceneArgs;
import org.semanticweb.swse.lucene.master.MasterLucene;
import org.semanticweb.swse.lucene.master.MasterLuceneArgs;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteLuceneBuild {
	private final static Logger _log = Logger.getLogger(RunRemoteLuceneBuild.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "remote nxz input file, can use % delimiter");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option redirO = new Option("redirs", "remote redirects file");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option gzredO = new Option("gzred", "remote redirects file is gzipped");
		gzredO.setArgs(0);
		gzredO.setRequired(false);
		options.addOption(gzredO);
		
		Option ranksO = new Option("ranks", "remote ranks nq.gz file, possibly with % delimiter");
		ranksO.setArgs(1);
		ranksO.setRequired(true);
		options.addOption(ranksO);
		
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
		
		boolean gzred = cmd.hasOption("gzred");
		
		String in = cmd.getOptionValue("in");
		String ranks = cmd.getOptionValue("ranks");
		String out = cmd.getOptionValue("out");
		String redirs = cmd.getOptionValue("redirs");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMILuceneConstants.DEFAULT_RMI_PORT);
		
		runRemoteLuceneBuild(in, ranks, servers, redirs, gzred, out);
	}

	public static void runRemoteLuceneBuild(String in, String ranks, RMIRegistries servers, String redirs, boolean gzred, String out) throws Exception {
		SlaveLuceneArgs sla = new SlaveLuceneArgs(in, ranks, redirs, out);
		sla.setGzRedirects(gzred);
		
		MasterLuceneArgs mla = new MasterLuceneArgs(sla);
		
		MasterLucene ml =  new MasterLucene();
		ml.startRemoteTask(servers, RMILuceneConstants.DEFAULT_STUB_NAME, mla);
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
		_log.info("Added "+r+" redirect pairs...");
		is.close();
		return r;
	}
}
