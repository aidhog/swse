package org.semanticweb.swse.ann.rank.master;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.deri.idrank.pagerank.OnDiskPageRank;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingConstants;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;
import org.semanticweb.swse.file.master.LocalWriteRemoteStreamThread;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterAnnRanker implements Master<MasterAnnRankingArgs>{
	public static final String RANKS_FILE = "ranks.nx.gz";
	private final static Logger _log = Logger.getLogger(MasterAnnRanker.class.getSimpleName());
	public static final int TICKS = 10000000;

	public MasterAnnRanker(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterAnnRankingArgs mra) throws Exception{
		RMIClient<RMIAnnRankingInterface> rmic = new RMIClient<RMIAnnRankingInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mra.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote ranking job with following args:");
		_log.log(Level.INFO, mra.toString());
		
		Collection<RMIAnnRankingInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		RMIUtils.mkdirsForFile(mra.getLocalRanks());
		
		_log.log(Level.INFO, "Initialising remote ranking...");
		Iterator<RMIAnnRankingInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteRankInitThread(stubIter.next(), i, servers, mra.getSlaveArgs(i), RMIAnnRankingConstants.DEFAULT_STUB_NAME);
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
		_log.log(Level.INFO, "...remote ranking initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Sorting by/extracting contexts...");
		RemoteSortByContextThread[] conts = new RemoteSortByContextThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<conts.length; i++){
			conts[i] = new RemoteSortByContextThread(stubIter.next(), i);
			conts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<conts.length; i++){
			conts[i].join();
			if(!conts[i].successful()){
				throw conts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" sorted/extracted...");
		}
		_log.log(Level.INFO, "...remote sorting/extracting contexts finished.");
		idletime = RMIThreads.idleTime(conts);
		_log.info("Total idle time for co-ordination on sorting/extracting "+idletime+"...");
		_log.info("Average idle time for co-ordination on sorting/extracting "+(double)idletime/(double)(conts.length)+"...");
		
		_log.log(Level.INFO, "Locally merging extracted contexts to "+mra.getLocalContexts()+" ...");
		NxParser[] iter = new NxParser[conts.length];
		InputStream[] iss = new InputStream[conts.length];
		for(int i=0; i<conts.length; i++){
			iss[i] = new GZIPInputStream(RemoteInputStreamClient.wrap(conts[i].getResult()));
			iter[i] = new NxParser(iss[i]);
		}
		
		MergeSortArgs msa = new MergeSortArgs(iter);
		MergeSortIterator msi = new MergeSortIterator(msa);
		
		OutputStream os = new FileOutputStream(mra.getLocalContexts());
		os = new GZIPOutputStream(os);
		Callback cb = new CallbackNxOutputStream(os);
		
		int c = 0;
		while(msi.hasNext()){
			c++;
			cb.processStatement(msi.next());
		}
		
		os.close();
		for(InputStream is:iss)
			is.close();
		_log.log(Level.INFO, "...locally merged "+c+" extracted contexts.");
		
		_log.log(Level.INFO, "Opening remote outputstreams ...");
		String remoteContextsFn = mra.getSlaveArgs().getOutDir() + "/" + MasterAnnRankingArgs.DEFAULT_CONTEXTS_FILENAME_GZ;
		RemoteOutputStreamThread[] rosts = new RemoteOutputStreamThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<rosts.length; i++){
			rosts[i] = new RemoteOutputStreamThread(stubIter.next(), i, RMIUtils.getLocalName(remoteContextsFn,i), false);
			rosts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rosts.length; i++){
			rosts[i].join();
			if(!rosts[i].successful()){
				throw rosts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" stream opened...");
		}
		_log.log(Level.INFO, "...remote opening output stream finished.");
		idletime = RMIThreads.idleTime(rosts);
		_log.info("Total idle time for co-ordination on opening output stream "+idletime+"...");
		_log.info("Average idle time for co-ordination on opening output stream "+(double)idletime/(double)(rosts.length)+"...");
		

		_log.log(Level.INFO, "...writing to remote output stream(s)...");

		ibts = new VoidRMIThread[stubs.size()];
		OutputStream oss[] = new OutputStream[stubs.size()]; 
		for(int j=0; j<ibts.length; j++){
			oss[j] = RemoteOutputStreamClient.wrap(rosts[j].getResult());
			ibts[j] = new LocalWriteRemoteStreamThread(new FileInputStream(mra.getLocalContexts()), oss[j], j);
			ibts[j].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int j=0; j<ibts.length; j++){
			ibts[j].join();
			if(!ibts[j].successful()){
				throw ibts[j].getException();
			}
			_log.log(Level.INFO, "..."+j+" written output stream...");
		}

		_log.log(Level.INFO, "...remote file flood done of local data.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on floodind file "+idletime+"...");
		_log.info("Average idle time for co-ordination on flooding file "+(double)idletime/(double)(ibts.length)+"...");
		
		_log.log(Level.INFO, "Closing remote outputstreams ...");
		for(OutputStream ros:oss)
			ros.close();
		_log.log(Level.INFO, "...remote closing output stream finished.");
		
		_log.log(Level.INFO, "Extracting/rewriting/pruning remote graph...");
		RemoteExtractGraphThread[] regts = new RemoteExtractGraphThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<conts.length; i++){
			regts[i] = new RemoteExtractGraphThread(stubIter.next(), i, RMIUtils.getLocalName(remoteContextsFn,i));
			regts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<regts.length; i++){
			regts[i].join();
			if(!regts[i].successful()){
				throw regts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" sorted/extracted...");
		}
		_log.log(Level.INFO, "...remote extraction/rewriting/pruning finished.");
		idletime = RMIThreads.idleTime(regts);
		_log.info("Total idle time for co-ordination on extraction/rewriting/pruning "+idletime+"...");
		_log.info("Average idle time for co-ordination on extraction/rewriting/pruning "+(double)idletime/(double)(regts.length)+"...");
		
		_log.log(Level.INFO, "Locally merging extracted graph fragments to "+mra.getLocalGraph()+" and "+mra.getLocalInvGraph()+" ...");
		for(int i=0; i<2; i++){
			for(int j=0; j<conts.length; j++){
				iss[j] = new GZIPInputStream(RemoteInputStreamClient.wrap(regts[j].getResult()[i]));
				iter[j] = new NxParser(iss[j]);
			}
			
			msa = new MergeSortArgs(iter);
			msi = new MergeSortIterator(msa);
			
			if(i==0)
				os = new FileOutputStream(mra.getLocalGraph());
			else os = new FileOutputStream(mra.getLocalInvGraph());
			
			os = new GZIPOutputStream(os);
			cb = new CallbackNxOutputStream(os);
			
			while(msi.hasNext()){
				cb.processStatement(msi.next());
			}
			
			os.close();
			for(InputStream is:iss)
				is.close();
		}
		
		_log.log(Level.INFO, "...locally merged extracted graphs.");
		
		_log.log(Level.INFO, "Locally ranking extracted graph stored at "+mra.getLocalGraph()+" and "+mra.getLocalInvGraph()+" and writing ranks to "+mra.getLocalRanks());

		OnDiskPageRank.ResetableNxInput graph = new OnDiskPageRank.ResetableNxInput(mra.getLocalGraph(), true);
		OnDiskPageRank.ResetableNxInput invgraph = new OnDiskPageRank.ResetableNxInput(mra.getLocalInvGraph(), true);
		
		OnDiskPageRank._ticks=TICKS;
		OnDiskPageRank._tmp_dir=mra.getTmpDir();
		OnDiskPageRank._damping=mra.getDamping();
		OnDiskPageRank._rank_iterations=mra.getIterations();
		
		OnDiskPageRank.rank(graph, invgraph, mra.getLocalRanks());
		
		_log.log(Level.INFO, "...locally ranked extracted graph with ranks stored at "+mra.getLocalRanks());
		
		_log.log(Level.INFO, "Opening remote outputstreams for sending ranks...");
		String remoteRanksFn = mra.getSlaveArgs().getOutDir() + MasterAnnRankingArgs.DEFAULT_RANKS_FILENAME_GZ;
		rosts = new RemoteOutputStreamThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<rosts.length; i++){
			rosts[i] = new RemoteOutputStreamThread(stubIter.next(), i, RMIUtils.getLocalName(remoteRanksFn,i), false);
			rosts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rosts.length; i++){
			rosts[i].join();
			if(!rosts[i].successful()){
				throw rosts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" stream opened...");
		}
		_log.log(Level.INFO, "...remote opening output stream finished.");
		idletime = RMIThreads.idleTime(rosts);
		_log.info("Total idle time for co-ordination on opening output stream "+idletime+"...");
		_log.info("Average idle time for co-ordination on opening output stream "+(double)idletime/(double)(rosts.length)+"...");
		

		_log.log(Level.INFO, "...writing to remote output stream(s)...");

		ibts = new VoidRMIThread[rosts.length];
		oss = new OutputStream[stubs.size()]; 
		for(int j=0; j<ibts.length; j++){
			oss[j] = RemoteOutputStreamClient.wrap(rosts[j].getResult());
			ibts[j] = new LocalWriteRemoteStreamThread(new FileInputStream(mra.getLocalRanks()), oss[j], j);
			ibts[j].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int j=0; j<ibts.length; j++){
			ibts[j].join();
			if(!ibts[j].successful()){
				throw ibts[j].getException();
			}
			_log.log(Level.INFO, "..."+j+" written output stream...");
		}

		_log.log(Level.INFO, "...remote file flood done of local data.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on floodind file "+idletime+"...");
		_log.info("Average idle time for co-ordination on flooding file "+(double)idletime/(double)(ibts.length)+"...");
		
		_log.log(Level.INFO, "Closing remote outputstreams ...");
		for(OutputStream ros:oss)
			ros.close();
		_log.log(Level.INFO, "...remote closing output stream finished.");
		
		_log.log(Level.INFO, "Ranking and scattering remote triples...");
		ibts = new RemoteRankScatterTriplesThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<conts.length; i++){
			ibts[i] = new RemoteRankScatterTriplesThread(stubIter.next(), i, RMIUtils.getLocalName(remoteRanksFn,i));
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" ranked/sorted/scattered...");
		}
		_log.log(Level.INFO, "...remote ranked/sorted/scattered finished.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on ranked/sorted/scattered "+idletime+"...");
		_log.info("Average idle time for co-ordination on ranked/sorted/scattered "+(double)idletime/(double)(ibts.length)+"...");
		
		_log.log(Level.INFO, "Gathering and aggregating remote ranked triples...");
		ibts = new RemoteAggregateRanksThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<conts.length; i++){
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
		
		_log.log(Level.INFO, "...distributed ranking finished.");
	}
}
