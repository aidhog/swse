package org.semanticweb.swse.file;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cli.SendFile;
import org.semanticweb.swse.file.RMIFileConstants;
import org.semanticweb.swse.file.RMIFileServer;

import junit.framework.TestCase;

public class RMISendFileTest extends TestCase{
	
	private static final String IN = "testdata/rankc/ranks.nx.gz";
	
	private static final String OUT = "testdata/index%/ranks.ris.nx.gz";
	
	private static final boolean USE_ROS = false;
	
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
					RMIFileServer.startRMIServer(host, port, RMIFileConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIFileConstants.DEFAULT_RMI_PORT);
					RMIFileServer.startRMIServer(s, RMIFileConstants.DEFAULT_RMI_PORT, RMIFileConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMIFileConstants.DEFAULT_RMI_PORT);
		
		SendFile.sendFile(IN, servers, OUT, USE_ROS);
	}
}
