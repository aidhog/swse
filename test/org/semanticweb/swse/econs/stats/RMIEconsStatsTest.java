package org.semanticweb.swse.econs.stats;

import junit.framework.TestCase;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.cli.RunRemoteStats;
import org.semanticweb.swse.econs.stats.RMIEconsStatsConstants;
import org.semanticweb.swse.econs.stats.RMIEconsStatsServer;
import org.semanticweb.yars.nx.parser.NxParser;

public class RMIEconsStatsTest extends TestCase{
	
	private static final String OUT = "testdata/econs/stats%/";
	
	private static final String IN= "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
	
	private static final boolean SKIP_BUILD = true;
	
	private static String[] SERVERS = new String[]{
			"localhost:1801",
			"localhost:1802",
			"localhost:1803",
			"localhost:1804"
	};
	
	public static void main(String[] args) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		for(String s:SERVERS){
			System.err.println("Setting up server "+s+"...");
			if(s.contains(":")){
				String[] sp = s.split(":");
				String host = sp[0];
				int port = Integer.parseInt(sp[1]);
				
				if(sp[0].equals("localhost")){
					RMIUtils.startRMIRegistry(port);
					RMIEconsStatsServer.startRMIServer(host, port, RMIEconsStatsConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIEconsStatsConstants.DEFAULT_RMI_PORT);
					RMIEconsStatsServer.startRMIServer(s, RMIEconsStatsConstants.DEFAULT_RMI_PORT, RMIEconsStatsConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIEconsStatsConstants.DEFAULT_RMI_PORT);

		RunRemoteStats.runRemoteStats(IN, GZ_IN, servers, OUT, SKIP_BUILD);
	}
}
