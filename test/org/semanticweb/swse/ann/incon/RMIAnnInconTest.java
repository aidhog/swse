package org.semanticweb.swse.ann.incon;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.cli.RunRemoteInconsistency;
import org.semanticweb.swse.ann.incon.RMIAnnInconConstants;
import org.semanticweb.swse.ann.incon.RMIAnnInconServer;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import junit.framework.TestCase;

public class RMIAnnInconTest extends TestCase{
	
	private static final String OUT = "testdata/ann/incon%/";
	
	private static final String INT = "testdata/ann/reasoned/tbox.nq.gz";
	private static final boolean GZ_INT = true;
	
	private static final String INA = "testdata/ann/agg%/data.final.nx.gz";
	private static final boolean GZ_INA = true;
	
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
					RMIAnnInconServer.startRMIServer(host, port, RMIAnnInconConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIAnnInconConstants.DEFAULT_RMI_PORT);
					RMIAnnInconServer.startRMIServer(s, RMIAnnInconConstants.DEFAULT_RMI_PORT, RMIAnnInconConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIAnnInconConstants.DEFAULT_RMI_PORT);
		
		RunRemoteInconsistency.runRemoteIncosistency(INA, GZ_INA, servers, INT, GZ_INT, OUT);
		
//		RedirectsAuthorityInspector rai = new RedirectsAuthorityInspector(r);
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/index.rdf")));
//		System.err.println(rai.checkAuthority(FOAF.PERSON, new Resource("http://xmlns.com/foaf/spec/")));
	}
	
}
