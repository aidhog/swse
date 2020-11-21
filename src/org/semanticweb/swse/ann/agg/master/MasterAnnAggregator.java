package org.semanticweb.swse.ann.agg.master;

import java.io.FileInputStream;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateConstants;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateInterface;
import org.semanticweb.swse.ann.agg.utils.RemoteScatter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterAnnAggregator implements Master<MasterAnnAggregatorArgs>{
	private final static Logger _log = Logger.getLogger(MasterAnnAggregator.class.getSimpleName());
	public static final int TICKS = 10000000;
	
	public MasterAnnAggregator(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterAnnAggregatorArgs maa) throws Exception{
		RMIClient<RMIAnnAggregateInterface> rmic = new RMIClient<RMIAnnAggregateInterface>(servers, stubName);
		
		RMIUtils.setLogFile(maa.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote aggregation job with following args:");
		_log.log(Level.INFO, maa.toString());
		
		Collection<RMIAnnAggregateInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		RMIUtils.mkdirs(maa.getTmpDir());
		
		_log.log(Level.INFO, "Initialising remote aggregation...");
		Iterator<RMIAnnAggregateInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAggInitThread(stubIter.next(), i, servers, maa.getSlaveArgs(i), RMIAnnAggregateConstants.DEFAULT_STUB_NAME);
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
		_log.log(Level.INFO, "...remote aggregation initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		
		_log.info("Sorting and scattering local data...");
		
		String data = maa.getLocal();
		long b4 = System.currentTimeMillis();
		
		InputStream is = null;
		Iterator<Node[]> input = null;
		is = new FileInputStream(data);
		if(maa.getGzLocal())
			is = new GZIPInputStream(is); 
			
		input = new NxParser(is);
		_log.info("...input from "+data);
		
		SortArgs sa = new SortArgs(input);
		sa.setComparator(new NodeComparator(true, true));
		sa.setTicks(TICKS);
		sa.setTmpDir(maa.getTmpDir());
		
		SortIterator si = new SortIterator(sa);
			
		RMIUtils.mkdirs(maa.getScatterDir());
		RemoteScatter.scatter(si, rmic, maa.getScatterDir(), maa.getRemoteGatherDir());
		
		_log.info("...data sorted and scattered in "+(System.currentTimeMillis()-b4)+" ms.");
		
		_log.log(Level.INFO, "Ranking and scattering remote triples...");
		ibts = new RemoteAggScatterThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAggScatterThread(stubIter.next(), i);
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
		
		_log.log(Level.INFO, "Gathering and aggregating remote ranked triples...");
		ibts = new RemoteAggregateRanksThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAggregateRanksThread(stubIter.next(), i);
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
	
}
