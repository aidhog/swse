package org.semanticweb.swse.task.master;

import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.task.RMITaskMngrInterface;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterTaskMngr implements Master<MasterTaskMngrArgs>{
	private final static Logger _log = Logger.getLogger(MasterTaskMngr.class.getSimpleName());
	
	private final static String STUB_NAME_PREFIX = "task";
	
	public MasterTaskMngr(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterTaskMngrArgs mba) throws Exception{
		
		RMIClient<RMITaskMngrInterface> rmic = new RMIClient<RMITaskMngrInterface>(servers, stubName);
		Collection<RMITaskMngrInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];
		
		RMIUtils.setLogFile(mba.getMasterLog());
		
		_log.log(Level.INFO, "Initialising remote ranking...");
		Iterator<RMITaskMngrInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteTaskMngrInitThread(stubIter.next(), i, servers);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" initialised...");
		}
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");
		
		
		_log.info("Starting "+mba.getTasks().length+" tasks as follows:");
		_log.info(mba.toString());
		
		long b4 = System.currentTimeMillis();
		for(int i=0; i<mba.getTasks().length; i++){
			
			MasterArgs ma = mba.getTasks()[i];
			_log.info("Starting task "+i+"... of type "+ma.getClass().getName());
			_log.info(ma.toString());
			
			stubs = rmic.getAllStubs();
			ibts = new RMIThread[stubs.size()];
			String taskStubName = STUB_NAME_PREFIX+i;
			
			_log.log(Level.INFO, "Initialising remote interface...");
			stubIter = stubs.iterator();
			for(int j=0; j<ibts.length; j++){
				ibts[j] = new RemoteTaskMngrBindThread(stubIter.next(), j, servers.getServer(j).getServerUrl(), servers.getServer(j).getPort(), taskStubName, ma.getRMIInterface(), false);
				ibts[j].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int j=0; j<ibts.length; j++){
				ibts[j].join();
				if(!ibts[j].successful()){
					throw ibts[j].getException();
				}
				_log.log(Level.INFO, "..."+j+" initialised...");
			}
			_log.log(Level.INFO, "...remote interfaces initialised.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
			_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");
			
			ma.getTaskMaster().startRemoteTask(servers, taskStubName, ma);
			
			_log.log(Level.INFO, "Unbinding remote interface...");
			stubIter = stubs.iterator();
			for(int j=0; j<ibts.length; j++){
				ibts[j] = new RemoteTaskMngrUnbindThread(stubIter.next(), j, servers.getServer(j).getServerUrl(), servers.getServer(j).getPort(), taskStubName);
				ibts[j].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int j=0; j<ibts.length; j++){
				ibts[j].join();
				if(!ibts[j].successful()){
					throw ibts[j].getException();
				}
				_log.log(Level.INFO, "..."+j+" unbound...");
			}
			_log.log(Level.INFO, "...remote interfaces unbound.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on unbinding "+idletime+"...");
			_log.info("Average idle time for co-ordination on unbinding "+(double)idletime/(double)(ibts.length)+"...");

			_log.info("Task "+i+"... of type "+ma.getClass().getName()+" done!");
		}
		
		rmic.clear();

		_log.log(Level.INFO, "...distributed tasks finished in "+(System.currentTimeMillis()-b4)+" ms.");
	}
}
