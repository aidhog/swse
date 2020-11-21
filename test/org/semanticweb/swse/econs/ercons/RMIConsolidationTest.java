package org.semanticweb.swse.econs.ercons;

import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.cli.RunRemoteConsolidation;
import org.semanticweb.swse.econs.ercons.RMIConsolidationConstants;
import org.semanticweb.swse.econs.ercons.RMIConsolidationServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.yars.nx.parser.NxParser;

public class RMIConsolidationTest extends TestCase{
	
	private static final String OUT = "testdata/econs/cons%/";
	
	private static final String REDIRS = "testdata/redirects.nx";
	private static final boolean GZ_RED = false;
	
	private static final String IN = "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
		
	private static final boolean REASON = false;
	
	private static final boolean SAMEAS_ONLY = true;
	
	private static String[] SERVERS = new String[]{
			"localhost:1801",
			"localhost:1802",
			"localhost:1803",
			"localhost:1804"
	};
	
	public static void main(String[] args) throws Exception{
		Logger log = Logger.getLogger(PldManager.class.getName());
		NxParser.DEFAULT_PARSE_DTS = false;
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
		
		RunRemoteConsolidation.runRemoteConsolidation(IN, GZ_IN, REDIRS, GZ_RED, REASON, SAMEAS_ONLY, servers, OUT);
	}
}
