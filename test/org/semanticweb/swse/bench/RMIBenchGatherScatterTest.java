package org.semanticweb.swse.bench;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.bench.RMIBenchConstants;
import org.semanticweb.swse.bench.RMIBenchServer;
import org.semanticweb.swse.cli.RunRemoteBench;

import junit.framework.TestCase;

public class RMIBenchGatherScatterTest extends TestCase{
	
	private static final String OUT = "testdata/benchg%/";
	
	private static final String IN = "testdata/reasoned%/data.r.nq.gz";
	
	private static final boolean GZIP = true;
	
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
					RMIBenchServer.startRMIServer(host, port, RMIBenchConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIBenchConstants.DEFAULT_RMI_PORT);
					RMIBenchServer.startRMIServer(s, RMIBenchConstants.DEFAULT_RMI_PORT, RMIBenchConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIBenchConstants.DEFAULT_RMI_PORT);
		
		RunRemoteBench.runBenchGatherScatter(IN, GZIP, servers, OUT);
	}
}
