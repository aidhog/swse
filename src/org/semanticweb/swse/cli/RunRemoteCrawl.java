package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ldspider.RMICrawlerConstants;
import org.semanticweb.swse.ldspider.master.MasterCrawler;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerSetup;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;

import com.ontologycentral.ldspider.CrawlerConstants;

/**
 * Main method to conduct a distributed crawl using remote crawlers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteCrawl {
	private final static Logger _log = Logger.getLogger(RunRemoteCrawl.class.getSimpleName());
	public final static String REDIRECTS_FILE = "redirs.2.nx"; 
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inputO = new Option("seeds", "file with list of seed urls");
		inputO.setArgs(1);
		inputO.setRequired(true);
		options.addOption(inputO);
		
		Option gzinO = new Option("gzseeds", "flag for gzipped seed file");
		gzinO.setArgs(0);
		options.addOption(gzinO);
		
		Option maxpldurisO = new Option("maxplduris", "maxuris per pld per crawler per round");
		maxpldurisO.setArgs(1);
		options.addOption(maxpldurisO);
		
		Option minpldurisO = new Option("minplduris", "minuris per pld per crawler per round");
		minpldurisO.setArgs(1);
		options.addOption(minpldurisO);
		
		Option targeturisO = new Option("targeturis", "target uris to crawl");
		targeturisO.setArgs(1);
		options.addOption(targeturisO);
		
		Option threadsO = new Option("threads", "threads per crawler");
		threadsO.setArgs(1);
		options.addOption(threadsO);
		
		Option roundsO = new Option("rounds", "number of rounds");
		roundsO.setArgs(1);
		options.addOption(roundsO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
//		Option logO = new Option("log", "log file, may contain a % delimiter which will be replaced by server index");
//		logO.setArgs(1);
//		options.addOption(logO);
		
		Option gzlogO = new Option("gzlog", "flag to gzip log");
		gzlogO.setArgs(0);
		options.addOption(gzlogO);
		
		Option scoreO = new Option("score", "flag to score PLDs by percent RDF");
		scoreO.setArgs(0);
		options.addOption(scoreO);
		
//		Option qO = new Option("qdir", "directory for queuq, may contain a % delimiter which will be replaced by server index");
//		qO.setArgs(1);
//		qO.setRequired(true);
//		options.addOption(qO);
		
		Option outO = new Option("out", "output dir, may contain a % delimiter which will be replaced by server index");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option gzoutO = new Option("gzout", "flag to gzip output");
		gzoutO.setArgs(0);
		options.addOption(gzoutO);
		
		Option gzredO = new Option("gzred", "flag to gzip redirects");
		gzredO.setArgs(0);
		options.addOption(gzredO);
		
		Option headO = new Option("headers", "extract RDF from headers");
		headO.setArgs(0);
		options.addOption(headO);
		
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
		
		int rounds = CrawlerConstants.DEFAULT_NB_ROUNDS;
		int threads = CrawlerConstants.DEFAULT_NB_THREADS;
		int maxplduris = CrawlerConstants.DEFAULT_MAX_PLD_ROUND;
		int minplduris = CrawlerConstants.DEFAULT_MIN_PLD_ROUND;
		int targeturis = CrawlerConstants.DEFAULT_MAX_URIS;
		
		
		if(cmd.hasOption("rounds")) 
			rounds = Integer.valueOf(cmd.getOptionValue("rounds"));
		if(cmd.hasOption("threads")) 
			threads = Integer.valueOf(cmd.getOptionValue("threads"));
		if(cmd.hasOption("maxplduris")) 
			maxplduris = Integer.valueOf(cmd.getOptionValue("maxplduris"));
		if(cmd.hasOption("minplduris")) 
			minplduris = Integer.valueOf(cmd.getOptionValue("minplduris"));
		if(cmd.hasOption("targeturis")) 
			targeturis = Integer.valueOf(cmd.getOptionValue("targeturis"));
		
		Set<String> seeds = null;
		File seedList = new File(cmd.getOptionValue("seeds"));
		if(!seedList.exists()) 
			throw new FileNotFoundException("No file found at "+seedList.getAbsolutePath());
		seeds = readSeeds(seedList, cmd.hasOption("gzseeds"));

		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMICrawlerConstants.DEFAULT_RMI_PORT);
		
		String out = cmd.getOptionValue("out");
		
		boolean gzout = cmd.hasOption("gzout");
		
		boolean gzlog = cmd.hasOption("gzlog");
		
		boolean score = cmd.hasOption("score");
		
		boolean gzred = cmd.hasOption("gzred");
		
		boolean head = cmd.hasOption("headers");
		
		String redirs = RMIUtils.getLocalName(out);
		redirs+="/"+REDIRECTS_FILE;
		if(gzred){
			redirs+=".gz";
		}
		
		runRemoteCrawl(seeds, servers, threads, rounds, maxplduris, minplduris, targeturis, out, gzout, gzlog, redirs, gzred, score, head);
	}
	
	
	public static void runRemoteCrawl(Set<String> seeds, RMIRegistries servers,
			int threads, int rounds, int maxplduris, int minplduris, int targeturis, String outdir, boolean gzout,
			boolean gzlog, String redirs, boolean gzred, boolean score, boolean extractHeaders) throws Exception {
		
		RMIUtils.mkdirs(RMIUtils.getLocalName(outdir));
		MasterCrawler mc =  new MasterCrawler(servers);
		
		RemoteCrawlerSetup[] rcss = new RemoteCrawlerSetup[servers.getServerCount()];
		for(int i=0; i<rcss.length; i++){
			rcss[i] = new RemoteCrawlerSetup(RMIUtils.getLocalName(outdir, i));
			rcss[i].setGzLog(gzlog);
			rcss[i].setGzData(gzout);
			rcss[i].setGzRedirects(gzred);
			rcss[i].setMinDelay(CrawlerConstants.DEFAULT_MIN_DELAY*servers.getServerCount());
			rcss[i].setMaxPldURIs(maxplduris);
			rcss[i].setMinPldURIs(minplduris);
			rcss[i].setThreads(threads);
			rcss[i].setScore(score);
			rcss[i].setExtractHeaders(extractHeaders);
		}
		
		mc.start(seeds, redirs, gzred, rcss, targeturis, rounds);
	}
	
	

	/**
	 * 
	 * @param q - queue
	 * @param seedList
	 * @throws IOException 
	 */
	public static Set<String> readSeeds(File seedList, boolean gzipped) throws IOException {
		Set<String> seeds = new HashSet<String>();
		
		InputStream is = new FileInputStream(seedList);
		if(gzipped){
			is = new GZIPInputStream(is);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(is, "utf-8"));
		String line=null;
		while((line=br.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty()){
				try {
					URI uri = LinkFilter.normalise(new URI(line));
					if(uri!=null){
						seeds.add(uri.toString());
					}
				} catch (URISyntaxException e) {
					_log.log(Level.FINE,"Discard invalid uri "+e.getMessage()+" for "+line);
				}
			}
		}
		br.close();
		return seeds;
	}
}
