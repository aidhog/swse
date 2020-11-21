package org.semanticweb.swse.cons;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteConsolidation;
import org.semanticweb.swse.cons.RMIConsolidationConstants;
import org.semanticweb.swse.cons.RMIConsolidationServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import junit.framework.TestCase;

public class RMIConsolidationTest extends TestCase{
	
	private static final String OUT = "testdata/cons%/";
	private static final boolean GZ_OUT = true;
	
	private static final String RANKS = "testdata/ranku/ranks.nx.gz";	
	private static final boolean GZ_RANKS = true;
	
	private static final String IN = "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
		
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
					RMIConsolidationServer.startRMIServer(host, port, RMIConsolidationConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIConsolidationConstants.DEFAULT_RMI_PORT);
					RMIConsolidationServer.startRMIServer(s, RMIConsolidationConstants.DEFAULT_RMI_PORT, RMIConsolidationConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIConsolidationConstants.DEFAULT_RMI_PORT);
		

		RunRemoteConsolidation.runRemoteConsolidation(IN, GZ_IN, servers, RANKS, GZ_RANKS, OUT, GZ_OUT);
	}
}
