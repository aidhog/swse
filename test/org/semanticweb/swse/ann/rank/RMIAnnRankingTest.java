package org.semanticweb.swse.ann.rank;

import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.deri.idrank.RankGraph;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.cli.RunRemoteRanking;
import org.semanticweb.swse.ann.rank.RMIAnnRankingConstants;
import org.semanticweb.swse.ann.rank.RMIAnnRankingServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

public class RMIAnnRankingTest extends TestCase{
	
	private static final String REDIRS = "testdata/out/redirs.nx";
	private static final boolean GZ_RED = false;
	
	private static final String OUT = "testdata/annranku%/";
	
	private static final String IN= "testdata/out%/data.nq";
	private static final boolean GZ_IN= false;
	
	private static final boolean TBOX = true;

	private static String[] SERVERS = new String[]{
			"localhost:1801",
			"localhost:1802",
			"localhost:1803",
			"localhost:1804"
	};
	
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
					RMIAnnRankingServer.startRMIServer(host, port, RMIAnnRankingConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIAnnRankingConstants.DEFAULT_RMI_PORT);
					RMIAnnRankingServer.startRMIServer(s, RMIAnnRankingConstants.DEFAULT_RMI_PORT, RMIAnnRankingConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIAnnRankingConstants.DEFAULT_RMI_PORT);
		
		int iters = RankGraph.ITERATIONS;
		double d = 0.85d;

		RunRemoteRanking.runRemoteRanking(IN, GZ_IN, servers, REDIRS, GZ_RED, iters, d, TBOX, OUT);
		
//		runLocal();
	}

//	private static void runLocal() {
//		String args[] = new String[]{
//				"-o", "testdata/rank/ranks.local.nx",
//				"-i", "testdata/data_all.nq",
//				"-r", REDIRS,
//				"-u",
//				"-p",
//				"-e"
//		};
//		org.deri.idrank.Main.main(args);
//	}
}
