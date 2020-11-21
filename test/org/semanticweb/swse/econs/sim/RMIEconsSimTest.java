package org.semanticweb.swse.econs.sim;

import junit.framework.TestCase;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.cli.RunRemoteSimilarity;
import org.semanticweb.swse.econs.sim.RMIEconsSimConstants;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer;
import org.semanticweb.yars.nx.parser.NxParser;

public class RMIEconsSimTest extends TestCase{
	
	private static final String OUT = "testdata/econs/sim%/";
	
	private static final String IN_S= "testdata/econs/statsc%/spoc.s.nq.gz";
	private static final boolean GZ_IN_S = true;
	
	private static final String IN_O= "testdata/econs/statsc%/opsc.s.nq.gz";
	private static final boolean GZ_IN_O = true;
	
	private static final String PS = "pred.stats.jo.gz";
	
	private static final String RDIR = "testdata/econs/sim%/gather%/";
	
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
					RMIEconsSimServer.startRMIServer(host, port, RMIEconsSimConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIEconsSimConstants.DEFAULT_RMI_PORT);
					RMIEconsSimServer.startRMIServer(s, RMIEconsSimConstants.DEFAULT_RMI_PORT, RMIEconsSimConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIEconsSimConstants.DEFAULT_RMI_PORT);

		RunRemoteSimilarity.runRemoteSimilarity(IN_S, GZ_IN_S, IN_O, GZ_IN_O, servers, OUT, PS, RDIR);
	}
}
