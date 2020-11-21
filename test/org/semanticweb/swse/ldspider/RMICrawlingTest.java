package org.semanticweb.swse.ldspider;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteCrawl;
import org.semanticweb.swse.ldspider.RMICrawlerConstants;
import org.semanticweb.swse.ldspider.RMICrawlerServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import junit.framework.TestCase;

public class RMICrawlingTest extends TestCase{
	
	private static final String SEEDS_FILE = "testdata/smallseeds.txt";
	private static final boolean SEEDS_GZIPPED = false;
	
//	private static final String SEEDS_FILE = "testdata/foaf_terms.txt";
//	private static final boolean SEEDS_GZIPPED = false;
	
	private static final String OUT_DIR = "testdata/out%/";
	private static final boolean OUT_GZIPPED = false;
	
	private static final String REDIRS = RMIUtils.getLocalName(OUT_DIR)+"/"+RunRemoteCrawl.REDIRECTS_FILE;
	
	private static final boolean LOG_GZIPPED = false;
	private static final boolean RED_GZIPPED = false;
	
	private static final boolean SCORE = false;
	
	private static final boolean HEADERS = false;
	
	private static final int THREADS = 2;
	private static final int ROUNDS = 5;
	private static final int TARGET_URIS = 125;
	
	private static final int MIN_PLD_URIS = 5;
	private static final int MAX_PLD_URIS = 10;
	
	private static String[] SERVERS = new String[]{
//			"deri-srvgal29.nuigalway.ie",
//			"deri-srvgal20.nuigalway.ie"
			"localhost:1801",
			"localhost:1802",
			"localhost:1803",
			"localhost:1804"
	};
	
//	public void testRemoteCrawl() throws IOException, ClassNotFoundException, AlreadyBoundException{
	public static void main(String[] args) throws Exception{
		Logger log = Logger.getLogger(PldManager.class.getName());
		log.setLevel(Level.WARNING);
		
		for(String s:SERVERS){
			System.err.println("Setting up server "+s+"...");
			if(s.contains(":")){
				String[] sp = s.split(":");
				String host = sp[0];
				int port = Integer.parseInt(sp[1]);
				
				if(sp[0].equals("localhost")){
					RMIUtils.startRMIRegistry(port);
					RMICrawlerServer.startRMIServer(host, port, RMICrawlerConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMICrawlerConstants.DEFAULT_RMI_PORT);
					RMICrawlerServer.startRMIServer(s, RMICrawlerConstants.DEFAULT_RMI_PORT, RMICrawlerConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		Set<String> seeds = null;
		File seedList = new File(SEEDS_FILE);
		if(!seedList.exists()) 
			throw new FileNotFoundException("No file found at "+seedList.getAbsolutePath());
		seeds = RunRemoteCrawl.readSeeds(seedList, SEEDS_GZIPPED);

		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMICrawlerConstants.DEFAULT_RMI_PORT);
		
		RunRemoteCrawl.runRemoteCrawl(seeds, servers, THREADS, ROUNDS, MAX_PLD_URIS, MIN_PLD_URIS, TARGET_URIS, OUT_DIR, OUT_GZIPPED, LOG_GZIPPED, REDIRS, RED_GZIPPED, SCORE, HEADERS);
	}
}
