package org.semanticweb.swse.hobo.stats.master;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.hobo.stats.PerPLDNamingStats;
import org.semanticweb.hobo.stats.PerPLDNamingStats.PerPLDStatsResults;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsConstants;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsInterface;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterHoboStats implements Master<MasterHoboStatsArgs>{
	private final static Logger _log = Logger.getLogger(MasterHoboStats.class.getSimpleName());
	public static final int TICKS = 10000000;
	
	public MasterHoboStats(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterHoboStatsArgs mesa) throws Exception{
		RMIClient<RMIHoboStatsInterface> rmic = new RMIClient<RMIHoboStatsInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mesa.getMasterLog());
		RMIUtils.mkdirsForFile(mesa.getStatsOut());
		
		_log.log(Level.INFO, "Setting up remote stats job with following args:");
		_log.log(Level.INFO, mesa.toString());
		
		Collection<RMIHoboStatsInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote aggregation...");
		Iterator<RMIHoboStatsInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteStatsInitThread(stubIter.next(), i, servers, mesa.getSlaveArgs(i), RMIHoboStatsConstants.DEFAULT_STUB_NAME);
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

		
		if(!mesa.getSkipBuild()){
//			_log.log(Level.INFO, "Sorting and scattering remote triples by subject...");
//			ibts = new RemoteStatsScatterThread[stubs.size()];
//			stubIter = stubs.iterator();
//			for(int i=0; i<ibts.length; i++){
//				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, MasterHoboStatsArgs.SPOC_ORDER, null);
//				ibts[i].start();
//			}
//	
//			_log.log(Level.INFO, "...awaiting thread return...");
//			for(int i=0; i<ibts.length; i++){
//				ibts[i].join();
//				if(!ibts[i].successful()){
//					throw ibts[i].getException();
//				}
//				_log.log(Level.INFO, "..."+i+" sorted/scattered...");
//			}
//			_log.log(Level.INFO, "...remote sorted/scattered finished.");
//			idletime = RMIThreads.idleTime(ibts);
//			_log.info("Total idle time for co-ordination on sorted/scattered "+idletime+"...");
//			_log.info("Average idle time for co-ordination on sorted/scattered "+(double)idletime/(double)(ibts.length)+"...");
//			
//			_log.log(Level.INFO, "Gathering and aggregating remote subject triples...");
//			ibts = new RemoteStatsAggTriplesThread[stubs.size()];
//			stubIter = stubs.iterator();
//			for(int i=0; i<ibts.length; i++){
//				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, MasterHoboStatsArgs.SPOC_ORDER, mesa.getRemoteOutSpoc());
//				ibts[i].start();
//			}
//	
//			_log.log(Level.INFO, "...awaiting thread return...");
//			for(int i=0; i<ibts.length; i++){
//				ibts[i].join();
//				if(!ibts[i].successful()){
//					throw ibts[i].getException();
//				}
//				_log.log(Level.INFO, "..."+i+" aggregated...");
//			}
//			_log.log(Level.INFO, "...remote aggregation finished.");
//			idletime = RMIThreads.idleTime(ibts);
//			_log.info("Total idle time for co-ordination on aggregation "+idletime+"...");
//			_log.info("Average idle time for co-ordination on aggregation "+(double)idletime/(double)(ibts.length)+"...");
//			
//			_log.log(Level.INFO, "...distributed aggregation finished.");
			
			_log.log(Level.INFO, "Sorting and scattering remote triples by subject...");
			ibts = new RemoteStatsScatterThread[stubs.size()];
			stubIter = stubs.iterator();
			TreeSet<Node> ignoreS = new TreeSet<Node>();
			
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, MasterHoboStatsArgs.SPOC_ORDER, ignoreS);
				ibts[i].start();
			}
	
			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" sorted/scattered...");
			}
			_log.log(Level.INFO, "...remote sorted/scattered finished.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sorted/scattered "+idletime+"...");
			_log.info("Average idle time for co-ordination on sorted/scattered "+(double)idletime/(double)(ibts.length)+"...");
			
			_log.log(Level.INFO, "Gathering and aggregating remote subject triples...");
			ibts = new RemoteStatsAggTriplesThread[stubs.size()];
			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, MasterHoboStatsArgs.SPOC_ORDER, mesa.getRemoteOutSpoc());
				ibts[i].start();
			}
	
			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" aggregated...");
			}
			_log.log(Level.INFO, "...remote aggregation finished.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on aggregation "+idletime+"...");
			_log.info("Average idle time for co-ordination on aggregation "+(double)idletime/(double)(ibts.length)+"...");
			
			_log.log(Level.INFO, "...distributed aggregation finished.");
			
			_log.log(Level.INFO, "Sorting and scattering remote triples by object...");
			
			TreeSet<Node> ignoreO = new TreeSet<Node>();
			ignoreO.add(RDF.TYPE);
			
			ibts = new RemoteStatsScatterThread[stubs.size()];
			stubIter = stubs.iterator();
			
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, MasterHoboStatsArgs.OPSC_ORDER, ignoreO);
				ibts[i].start();
			}
	
			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" sorted/scattered...");
			}
			_log.log(Level.INFO, "...remote sorted/scattered finished.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sorted/scattered "+idletime+"...");
			_log.info("Average idle time for co-ordination on sorted/scattered "+(double)idletime/(double)(ibts.length)+"...");
			
			_log.log(Level.INFO, "Gathering and aggregating remote object triples...");
			ibts = new RemoteStatsAggTriplesThread[stubs.size()];
			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, MasterHoboStatsArgs.OPSC_ORDER, mesa.getRemoteOutOpsc());
				ibts[i].start();
			}
	
			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" aggregated...");
			}
			_log.log(Level.INFO, "...remote aggregation finished.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on aggregation "+idletime+"...");
			_log.info("Average idle time for co-ordination on aggregation "+(double)idletime/(double)(ibts.length)+"...");
			
			_log.log(Level.INFO, "...distributed aggregation finished.");
		}
		
		_log.log(Level.INFO, "Generating remote statistics...");
		RemoteStatsThread[] rsts = new RemoteStatsThread[stubs.size()];
		stubIter = stubs.iterator();
		
//		Set<Node> skip = null;
//		if(mesa.getIgnoreSameAs()){
//			skip = new HashSet<Node>();
//			skip.add(OWL.SAMEAS);
//		}
		for(int i=0; i<rsts.length; i++){
			rsts[i] = new RemoteStatsThread(stubIter.next(), i, mesa.getRemoteOutSpoc(), mesa.getRemoteOutOpsc());
			rsts[i].start();
		}
		
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rsts.length; i++){
			rsts[i].join();
			if(!rsts[i].successful()){
				throw rsts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" stats done...");
		}
		_log.log(Level.INFO, "...remote stats finished.");
		idletime = RMIThreads.idleTime(rsts);
		_log.info("Total idle time for co-ordination on stats "+idletime+"...");
		_log.info("Average idle time for co-ordination on stats "+(double)idletime/(double)(rsts.length)+"...");
		
		HashMap<String,PerPLDStatsResults> srs = new HashMap<String,PerPLDStatsResults>();
		
		_log.info("Merging stats...");
		
		for(RemoteStatsThread rst:rsts){
			for(Entry<String,PerPLDStatsResults> e: rst.getResult().entrySet()){
				PerPLDStatsResults ppsr = srs.get(e.getKey());
				if(ppsr==null){
					srs.put(e.getKey(), e.getValue());
				} else{
					ppsr.addStats(e.getValue());
				}
			}
		}
		
		_log.info("Calculating overlaps...");
		PerPLDNamingStats.computeOverlap(srs);
		
		_log.info("Logging stats...");
		
		int i = PerPLDNamingStats.log(srs, new LoggerCallback(_log, Level.INFO));
		
		_log.info("...wrote "+i+" results ms.");
		
		_log.info("...serialising...");
		OutputStream ss = new GZIPOutputStream(new FileOutputStream(mesa.getStatsOut()));
		PerPLDNamingStats.serialise(srs, ss);
		ss.close();
		_log.info("...done.");
	}
	
	public static class LoggerCallback implements Callback{
		Logger _log;
		Level _l;
		
		public LoggerCallback(Logger log, Level l){
			_log = log;
			
			if(l==null) _l = Level.INFO;
			else _l = l;
		}

		public void endDocument() {
			;
		}

		public void processStatement(Node[] arg0) {
			_log.log(_l, Nodes.toN3(arg0));
		}

		public void startDocument() {
			;
		}
	}
	
}
