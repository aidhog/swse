package org.semanticweb.swse.ann.agg;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateConstants;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateServer;
import org.semanticweb.swse.ann.cli.RunRemoteAggregation;

import junit.framework.TestCase;

public class RMIAnnAggregationTest extends TestCase{
	
	private static final String RAW = "testdata/annranku%/data.nq.s.r.nx.gz";
	private static final boolean GZ_RAW = true;
	
	private static final String OUT = "testdata/ann/agg%/";
	
	private static final String IN= "testdata/ann/reasoned%/data.r.nq.gz";
	private static final boolean GZ_IN= true;
	
	private static final String LOCAL= "testdata/ann/reasoned/tbox.reason.nq.gz";
	private static final boolean GZ_LOCAL= true;
	
	private static String[] SERVERS = new String[]{
			"localhost:1801",
			"localhost:1802",
			"localhost:1803",
			"localhost:1804"
	};
	
	public static void main(String[] args) throws Exception{
		for(String s:SERVERS){
			System.err.println("Setting up server "+s+"...");
			if(s.contains(":")){
				String[] sp = s.split(":");
				String host = sp[0];
				int port = Integer.parseInt(sp[1]);
				
				if(sp[0].equals("localhost")){
					RMIUtils.startRMIRegistry(port);
					RMIAnnAggregateServer.startRMIServer(host, port, RMIAnnAggregateConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIAnnAggregateConstants.DEFAULT_RMI_PORT);
					RMIAnnAggregateServer.startRMIServer(s, RMIAnnAggregateConstants.DEFAULT_RMI_PORT, RMIAnnAggregateConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIAnnAggregateConstants.DEFAULT_RMI_PORT);

		RunRemoteAggregation.runRemoteAggregation(IN, GZ_IN, servers, RAW, GZ_RAW, LOCAL, GZ_LOCAL, OUT);
	}
}
