package org.semanticweb.swse.rank;

import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.deri.idrank.RankGraph;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteRanking;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.rank.RMIRankingConstants;
import org.semanticweb.swse.rank.RMIRankingServer;

public class RMIRankingTest extends TestCase{
	
	private static final String REDIRS = "testdata/out/redirs.nx";
	private static final boolean GZ_RED = false;
	
//	private static final String OUT = "testdata/ranku%/";
	private static final String OUT = "testdata/rankc%/";
	
	private static final String IN_NA = "testdata/out%/data.nq";
	private static final boolean GZ_IN_NA = false;
	
//	private static final String IN_ID = "testdata/out%/data.nq";
//	private static final boolean GZ_IN_ID = false;
	
	private static final String IN_ID = "testdata/cons%/data.cons.nq.gz";
	private static final boolean GZ_IN_ID = true;
	
	private static final boolean TBOX = false;
	private static final boolean PLDS = true;
	
	private static final boolean FLOOD = false;
		
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
					RMIRankingServer.startRMIServer(host, port, RMIRankingConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIRankingConstants.DEFAULT_RMI_PORT);
					RMIRankingServer.startRMIServer(s, RMIRankingConstants.DEFAULT_RMI_PORT, RMIRankingConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIRankingConstants.DEFAULT_RMI_PORT);
		
		int iters = RankGraph.ITERATIONS;
		float d = RankGraph.DAMPING;

		RunRemoteRanking.runRemoteRanking(IN_NA, GZ_IN_NA, IN_ID, GZ_IN_ID, servers, REDIRS, GZ_RED, iters, d, PLDS, TBOX, OUT, FLOOD);
		
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
