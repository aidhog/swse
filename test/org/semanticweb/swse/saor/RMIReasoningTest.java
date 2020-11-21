package org.semanticweb.swse.saor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteReasoning;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.saor.RMIReasonerConstants;
import org.semanticweb.swse.saor.RMIReasonerServer;

import junit.framework.TestCase;

public class RMIReasoningTest extends TestCase{
	
	private static final String RREDIRS = "testdata/redirects.nx";
	private static final boolean GZ_RRED = false;
	
	private static final String LREDIRS = RREDIRS;
	private static final boolean GZ_LRED = GZ_RRED;
	
	private static final String OUT = "testdata/reasoned%/";
	
	private static final String INT = "testdata/out%/data.nq";
	private static final boolean GZ_INT = false;
	
	private static final String INA = "testdata/out%/data.nq";
	private static final boolean GZ_INA = false;
	
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
					RMIReasonerServer.startRMIServer(host, port, RMIReasonerConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIReasonerConstants.DEFAULT_RMI_PORT);
					RMIReasonerServer.startRMIServer(s, RMIReasonerConstants.DEFAULT_RMI_PORT, RMIReasonerConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIReasonerConstants.DEFAULT_RMI_PORT);
		
		RunRemoteReasoning.runRemoteReasoning(INT, GZ_INT, INA, GZ_INA, servers, LREDIRS, GZ_LRED, OUT);
		
//		RedirectsAuthorityInspector rai = new RedirectsAuthorityInspector(r);
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/index.rdf")));
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/")));
	}
	
}
