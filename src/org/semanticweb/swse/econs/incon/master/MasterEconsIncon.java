package org.semanticweb.swse.econs.incon.master;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.swse.econs.incon.RMIEconsInconInterface;
import org.semanticweb.swse.econs.stats.RMIEconsStatsConstants;
import org.semanticweb.yars.nx.Node;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterEconsIncon implements Master<MasterEconsInconArgs>{
	private final static Logger _log = Logger.getLogger(MasterEconsIncon.class.getSimpleName());
	public static final int TICKS = 10000000;
	
	public MasterEconsIncon(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterEconsInconArgs meia) throws Exception{
		RMIClient<RMIEconsInconInterface> rmic = new RMIClient<RMIEconsInconInterface>(servers, stubName);
		
		RMIUtils.setLogFile(meia.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote stats job with following args:");
		_log.log(Level.INFO, meia.toString());
		
		Collection<RMIEconsInconInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote aggregation...");
		Iterator<RMIEconsInconInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteInconInitThread(stubIter.next(), i, servers, meia.getSlaveArgs(i), RMIEconsStatsConstants.DEFAULT_STUB_NAME);
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
		_log.log(Level.INFO, "...remote stats initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		
		_log.log(Level.INFO, "Detecting remote inconsistencies...");
		RemoteInconThread[] rsts = new RemoteInconThread[stubs.size()];
		stubIter = stubs.iterator();
		
		for(int i=0; i<rsts.length; i++){
			rsts[i] = new RemoteInconThread(stubIter.next(), i);
			rsts[i].start();
		}
		
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rsts.length; i++){
			rsts[i].join();
			if(!rsts[i].successful()){
				throw rsts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" incons done...");
		}
		_log.log(Level.INFO, "...remote inconsistencies finished.");
		idletime = RMIThreads.idleTime(rsts);
		_log.info("Total idle time for co-ordination on incons "+idletime+"...");
		_log.info("Average idle time for co-ordination on incons "+(double)idletime/(double)(rsts.length)+"...");
		
		_log.log(Level.INFO, "Merging repairs...");
		long b4 = System.currentTimeMillis();
		Map<Node,Map<Node,SameAsList>> repairs = new HashMap<Node,Map<Node,SameAsList>>();
		for(int i=0; i<rsts.length; i++){
			repairs.putAll(rsts[i].getResult());
		}
		OutputStream os = new FileOutputStream("repairs.jo.gz");
		os = new GZIPOutputStream(os);
		ObjectOutputStream oos = new ObjectOutputStream(os);
		oos.writeObject(repairs);
		oos.close();
		_log.info("...done repair merge in "+(System.currentTimeMillis()-b4)+" ms.");
		
		int split = 0;
		for(Map<Node,SameAsList> part: repairs.values()){
			if(part.isEmpty()) split++;
		}
		_log.info("....repairing "+repairs.size()+" partitions of which "+split+" must be completely disbanded.");
		
		_log.log(Level.INFO, "Repairing remote inconsistencies...");
		RemoteInconRepairThread[] rirs = new RemoteInconRepairThread[stubs.size()];
		stubIter = stubs.iterator();
		
		for(int i=0; i<rirs.length; i++){
			rirs[i] = new RemoteInconRepairThread(stubIter.next(), i, repairs);
			rirs[i].start();
		}
		
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rirs.length; i++){
			rirs[i].join();
			if(!rirs[i].successful()){
				throw rirs[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" incons repair done...");
		}
		_log.log(Level.INFO, "...remote inconsistency repair finished.");
		idletime = RMIThreads.idleTime(rirs);
		_log.info("Total idle time for co-ordination on incons repair "+idletime+"...");
		_log.info("Average idle time for co-ordination on incons repair "+(double)idletime/(double)(rirs.length)+"...");
		
		_log.info("...finished!!!");
	}
	
}
