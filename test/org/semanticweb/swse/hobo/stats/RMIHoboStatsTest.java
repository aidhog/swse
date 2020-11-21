package org.semanticweb.swse.hobo.stats;

import junit.framework.TestCase;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.hobo.cli.RunRemoteStats;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsConstants;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsServer;
import org.semanticweb.yars.nx.parser.NxParser;

public class RMIHoboStatsTest extends TestCase{
	
	private static final String OUT = "testdata/hobo/stats%/";
	
	private static final String IN= "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
	
	private static final String A= "testdata/out/access.log";
	private static final boolean GZ_A = false;
	
	private static final String R= "testdata/out/redirects.nx";
	private static final boolean GZ_R= false;
	
	private static final String C= "testdata/out/contexts.s.all.nx";
	private static final boolean GZ_C= false;
	
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
					RMIHoboStatsServer.startRMIServer(host, port, RMIHoboStatsConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIHoboStatsConstants.DEFAULT_RMI_PORT);
					RMIHoboStatsServer.startRMIServer(s, RMIHoboStatsConstants.DEFAULT_RMI_PORT, RMIHoboStatsConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIHoboStatsConstants.DEFAULT_RMI_PORT);

		RunRemoteStats.runRemoteStats(IN, GZ_IN, A, GZ_A, R, GZ_R, C, GZ_C, servers, OUT, SKIP_BUILD);
	}
}
