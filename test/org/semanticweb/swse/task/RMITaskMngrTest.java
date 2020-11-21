package org.semanticweb.swse.task;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.task.RMITaskMngrConstants;
import org.semanticweb.swse.task.RMITaskMngrServer;
import org.semanticweb.swse.task.master.MasterTaskMngr;
import org.semanticweb.swse.task.master.MasterTaskMngrArgs;
import org.semanticweb.swse.tasks.Tasks;

import junit.framework.TestCase;

public class RMITaskMngrTest extends TestCase{
	
	private static final String RREDIRS = "testdata/redirects.nx";
	private static final boolean GZ_RRED = false;
	
	private static final String LREDIRS = RREDIRS;
	private static final boolean GZ_LRED = GZ_RRED;
	
	private static final String OUT = "testdata/swse%/";
	
	private static final String IN = "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
	
	private static final MasterArgs[] TASKS = 
		Tasks.createTask(IN, GZ_IN, LREDIRS, GZ_LRED, RREDIRS, GZ_RRED, OUT);
	
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
					RMITaskMngrServer.startRMIServer(host, port, RMITaskMngrConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMITaskMngrConstants.DEFAULT_RMI_PORT);
					RMITaskMngrServer.startRMIServer(s, RMITaskMngrConstants.DEFAULT_RMI_PORT, RMITaskMngrConstants.DEFAULT_STUB_NAME);
				}
				
			}
			System.err.println("...set up server "+s);
		}
		
		
		RMIRegistries servers = new RMIRegistries(SERVERS, RMITaskMngrConstants.DEFAULT_RMI_PORT);
		MasterTaskMngrArgs mtma = new MasterTaskMngrArgs(TASKS);
		
		MasterTaskMngr mtm = new MasterTaskMngr();
		mtm.startRemoteTask(servers, RMITaskMngrConstants.DEFAULT_STUB_NAME, mtma);
	}
}
