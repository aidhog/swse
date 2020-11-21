package org.semanticweb.swse.econs.stats.master;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.stats.RMIEconsStatsConstants;
import org.semanticweb.swse.econs.stats.RMIEconsStatsInterface;
import org.semanticweb.swse.econs.stats.RMIEconsStatsServer.StatsResults;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterEconsStats implements Master<MasterEconsStatsArgs>{
	private final static Logger _log = Logger.getLogger(MasterEconsStats.class.getSimpleName());
	public static final int TICKS = 10000000;
	
	public MasterEconsStats(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterEconsStatsArgs mesa) throws Exception{
		RMIClient<RMIEconsStatsInterface> rmic = new RMIClient<RMIEconsStatsInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mesa.getMasterLog());
		RMIUtils.mkdirsForFile(mesa.getStatsOut());
		
		_log.log(Level.INFO, "Setting up remote stats job with following args:");
		_log.log(Level.INFO, mesa.toString());
		
		Collection<RMIEconsStatsInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote aggregation...");
		Iterator<RMIEconsStatsInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteStatsInitThread(stubIter.next(), i, servers, mesa.getSlaveArgs(i), RMIEconsStatsConstants.DEFAULT_STUB_NAME);
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
//				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, MasterEconsStatsArgs.SPOC_ORDER, null);
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
//				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, MasterEconsStatsArgs.SPOC_ORDER, mesa.getRemoteOutSpoc());
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
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, mesa.getSpocOrder(), null);
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
				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, mesa.getSpocOrder(), mesa.getRemoteOutSpoc());
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
			TreeSet<Node> ignore = new TreeSet<Node>();
			ignore.add(RDF.TYPE);
			ibts = new RemoteStatsScatterThread[stubs.size()];
			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteStatsScatterThread(stubIter.next(), i, mesa.getOpscOrder(), ignore);
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
				ibts[i] = new RemoteStatsAggTriplesThread(stubIter.next(), i, mesa.getOpscOrder(), mesa.getRemoteOutOpsc());
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
		
		Set<Node> skip = null;
		if(mesa.getIgnoreSameAs()){
			skip = new HashSet<Node>();
			skip.add(OWL.SAMEAS);
		}
		for(int i=0; i<rsts.length; i++){
			rsts[i] = new RemoteStatsThread(stubIter.next(), i, mesa.getRemoteOutSpoc(), mesa.getRemoteOutOpsc(), skip);
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
		
		StatsResults srs = new StatsResults();
		
		_log.info("Logging stats...");
		
		for(RemoteStatsThread rst:rsts){
			srs.addStatsResults(rst.getResult());
		}
		
		rsts = null;
		
		srs.logStats(_log, Level.INFO);
		
		_log.info("Serialising stats to "+mesa.getStatsOut()+" ...");
		ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(mesa.getStatsOut())));
		oos.writeObject(srs);
		oos.close();
		_log.info("...done.");
	}
	
}
