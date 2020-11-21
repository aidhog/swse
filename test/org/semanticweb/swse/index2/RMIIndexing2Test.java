package org.semanticweb.swse.index2;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteIndexing2;
import org.semanticweb.swse.index2.RMIIndexerConstants;
import org.semanticweb.swse.index2.RMIIndexerServer;

import junit.framework.TestCase;

public class RMIIndexing2Test extends TestCase{
	
	private static final String OUT = "testdata/indextwo%/";
	
	private static final String[] R_IN = new String[]{
		"testdata/cons%/data.cons.nq.gz" , "testdata/reasoned%/data.r.nq.gz"
	};
	
	private static final boolean[] GZ_R_IN = new boolean[]{
		true, true
	};
	
	private static final String[] L_IN = new String[]{
		"testdata/reasoned/tbox.r.nq.gz"
	};
	
	private static final boolean[] GZ_L_IN = new boolean[]{
		true
	};
	
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
					RMIIndexerServer.startRMIServer(host, port, RMIIndexerConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIIndexerConstants.DEFAULT_RMI_PORT);
					RMIIndexerServer.startRMIServer(s, RMIIndexerConstants.DEFAULT_RMI_PORT, RMIIndexerConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIIndexerConstants.DEFAULT_RMI_PORT);
		
		RunRemoteIndexing2.runRemoteIndexing(L_IN, GZ_L_IN, R_IN, GZ_R_IN, servers, OUT);
	}
}
