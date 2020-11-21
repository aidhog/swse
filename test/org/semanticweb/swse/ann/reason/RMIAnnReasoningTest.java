package org.semanticweb.swse.ann.reason;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.cli.RunRemoteReasoning;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerConstants;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import junit.framework.TestCase;

public class RMIAnnReasoningTest extends TestCase{
	
	private static final String RREDIRS = "testdata/redirects.nx";
	private static final boolean GZ_RRED = false;
	
	private static final String LREDIRS = RREDIRS;
	private static final boolean GZ_LRED = GZ_RRED;
	
	private static final String OUT = "testdata/ann/reasoned%/";
	
	private static final String INT = "testdata/annranku%/data.c.s.nq.gz";
	private static final boolean GZ_INT = true;
	
	private static final String INA = "testdata/annranku%/data.nq.s.r.nx.gz";
	private static final boolean GZ_INA = true;
	
	private static final String INRANKS = "testdata/annranku/ranks.all.nx.gz";
	private static final boolean GZ_INRANKS = true;
	
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
					RMIAnnReasonerServer.startRMIServer(host, port, RMIAnnReasonerConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIAnnReasonerConstants.DEFAULT_RMI_PORT);
					RMIAnnReasonerServer.startRMIServer(s, RMIAnnReasonerConstants.DEFAULT_RMI_PORT, RMIAnnReasonerConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIAnnReasonerConstants.DEFAULT_RMI_PORT);
		
		RunRemoteReasoning.runRemoteReasoning(INT, GZ_INT, INA, GZ_INA, servers, LREDIRS, GZ_LRED, INRANKS, GZ_INRANKS, OUT);
		
//		RedirectsAuthorityInspector rai = new RedirectsAuthorityInspector(r);
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/index.rdf")));
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/")));
	}
	
}
