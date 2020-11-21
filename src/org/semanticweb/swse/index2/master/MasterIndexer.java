package org.semanticweb.swse.index2.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index2.RMIIndexerInterface;
import org.semanticweb.swse.index2.utils.RemoteScatter;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterIndexer implements Master<MasterIndexerArgs>{
	private final static Logger _log = Logger.getLogger(MasterIndexer.class.getSimpleName());
	public static final String TBOX_REASONING_FILE = "tbox.r.nq.gz";
	public static final String TBOX_FILE = "tbox.nq.gz";
	
	public static final String SPLIT_FILE = "split.nq.gz";

	public MasterIndexer(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterIndexerArgs mia) throws Exception{
		RMIClient<RMIIndexerInterface> rmic = new RMIClient<RMIIndexerInterface>(servers, stubName);
		RMIUtils.setLogFile(mia.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote indexing job with following args:");
		_log.log(Level.INFO, mia.toString());
		
		Collection<RMIIndexerInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		RMIUtils.mkdirs(mia.getScatterDir());
		
		_log.log(Level.INFO, "Initialising remote indexing...");
		Iterator<RMIIndexerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerInitThread(stubIter.next(), i, servers, mia.getSlaveArgs(i), stubName);
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
		_log.log(Level.INFO, "...remote indexers initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		ArrayList<VoidRMIThread> gatherThreads = new ArrayList<VoidRMIThread>();
		if(mia.getLocalFiles()!=null && mia.getLocalFiles().length>0){
			long b4 = System.currentTimeMillis();
			
			_log.log(Level.INFO, "Scattering local file(s) first...");
			
			RemoteScatter.scatter(mia.getLocalFiles(), mia.getGzLocal(), rmic, mia.getScatterDir(), SPLIT_FILE);
			
			_log.info("scattering local files took "+(System.currentTimeMillis()-b4)+" ms.");
		}
		
		_log.log(Level.INFO, "Scattering remote...");

		stubIter = stubs.iterator();

		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerScatterThread(stubIter.next(), i, RMIUtils.getLocalNames(mia.getRemoteFiles(), i), mia.getGzRemote());
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return for remote scatter...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" remote scattered...");
		}
		_log.log(Level.INFO, "...remote threads scattered received.");

		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Running remote index build...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerMakeIndexThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" index built...");
		}
		_log.log(Level.INFO, "...remote index build done.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on indexing "+idletime+"...");
		_log.info("Average idle time for co-ordination on indexing "+(double)idletime/(double)(ibts.length)+"...");

		rmic.clear();
		
		_log.log(Level.INFO, "...distributed indexing finished.");
	}
}
