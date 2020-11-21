package org.semanticweb.swse.lucene.master;

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
import org.semanticweb.swse.lucene.RMILuceneInterface;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterLucene implements Master<MasterLuceneArgs>{
	private final static Logger _log = Logger.getLogger(MasterLucene.class.getSimpleName());

	public MasterLucene(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterLuceneArgs mla) throws Exception{
		RMIClient<RMILuceneInterface> rmic = new RMIClient<RMILuceneInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mla.getMasterLog());
		
		_log.info("Starting lucene index build...");
		
		Collection<RMILuceneInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote lucene build...");
		Iterator<RMILuceneInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteLuceneInitThread(stubIter.next(), i, servers, mla.getSlaveArgs(i));
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

		_log.log(Level.INFO, "Running remote lucene build...");
		
		stubIter = stubs.iterator();

		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteLuceneBuildThread(stubIter.next(), i);
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
		_log.log(Level.INFO, "...remote lucene index build done.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on indexing "+idletime+"...");
		_log.info("Average idle time for co-ordination on indexing "+(double)idletime/(double)(ibts.length)+"...");

		rmic.clear();
		
		_log.log(Level.INFO, "...distributed indexing finished.");
	}
}
