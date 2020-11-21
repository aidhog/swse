package org.semanticweb.swse.lucene;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.RunRemoteLuceneBuild;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.lucene.RMILuceneConstants;
import org.semanticweb.swse.lucene.RMILuceneServer;

import junit.framework.TestCase;

public class RMILuceneTest extends TestCase{
	
	private static final String REDIRS = "testdata/redirects.nx";
	private static final boolean GZ_RED = false;
	
	private static final String OUT = "testdata/index%/lucene/";
	
	private static final String IN = "testdata/index%/spoc.nqz";
	
	private static final String RANKS = "testdata/ranku%/ranks.nx.gz";
	
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
					RMILuceneServer.startRMIServer(host, port, RMILuceneConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMILuceneConstants.DEFAULT_RMI_PORT);
					RMILuceneServer.startRMIServer(s, RMILuceneConstants.DEFAULT_RMI_PORT, RMILuceneConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMILuceneConstants.DEFAULT_RMI_PORT);
		
		RunRemoteLuceneBuild.runRemoteLuceneBuild(IN, RANKS, servers, REDIRS, GZ_RED, OUT);
	}
}
