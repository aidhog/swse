package org.semanticweb.swse.cons.master;

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

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.RMIConsolidationInterface;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterConsolidation implements Master<MasterConsolidationArgs> {
	private final static Logger _log = Logger.getLogger(MasterConsolidation.class.getSimpleName());

	public static final String SAME_AS_TEMP = "sameas.nq.gz";
	
	public static final int TOP_K = 10;
	public static final int RAND_K = 100;

	private final static int[] ELS = new int[]{0,2};
//	private RMIRegistries _servers;
//	private RMIClient<RMIConsolidationInterface> _rmic;

	public MasterConsolidation(){
		;
	}
	
	public void startRemoteTask(RMIRegistries servers, String stubName, MasterConsolidationArgs mca) throws Exception{
		RMIClient<RMIConsolidationInterface> rmic = new RMIClient<RMIConsolidationInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mca.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote consolidation job with following args:");
		_log.log(Level.INFO, mca.toString());
		
		RMIUtils.mkdirsForFile(mca.getSameAsOut());
		
		Collection<RMIConsolidationInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote consolidation...");
		Iterator<RMIConsolidationInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidationInitThread(stubIter.next(), i, servers, mca.getSlaveArgs(i));
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
		_log.log(Level.INFO, "...remote consolidation initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Getting sameas pairs...");

		stubIter = stubs.iterator();
		RemoteConsolidationSameasThread[] rcss = new RemoteConsolidationSameasThread[stubs.size()];

		for(int i=0; i<rcss.length; i++){
			rcss[i] = new RemoteConsolidationSameasThread(stubIter.next(), i);
			rcss[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		InputStream[] iss = new InputStream[rcss.length];
		for(int i=0; i<rcss.length; i++){
			rcss[i].join();
			if(!rcss[i].successful()){
				throw rcss[i].getException();
			}
			iss[i] = RemoteInputStreamClient.wrap(rcss[i].getResult());
			_log.log(Level.INFO, "...sameas pairs received from "+i+"...");
		}
		_log.log(Level.INFO, "...sameas pairs received.");

		idletime = RMIThreads.idleTime(rcss);
		_log.info("Total idle time for co-ordination on sameas pairs "+idletime+"...");
		_log.info("Average idle time for co-ordination on sameas pairs "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating same as pairs and writing output to "+mca.getSameAsOut()+"...");
		long time = System.currentTimeMillis();
		NxParser[] nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		OutputStream os = new FileOutputStream(mca.getSameAsOut());
		if(mca.getGzSameAsOut()){
			os = new GZIPOutputStream(os);
		}
		CallbackNxOutputStream cb = new CallbackNxOutputStream(os);
		
		InputStream ris = null;
		Iterator<Node[]> riter = null;
		if(mca.getRanks()!=null){
			ris = new FileInputStream(mca.getRanks());
			if(mca.getGzRanks())
				ris = new GZIPInputStream(ris);
			riter = new NxParser(ris);
		}
		
		SameAsIndex sai = buildSameAsIndex(cb, nxps, riter);
		if(ris!=null){
			ris.close();
		}
		os.close();

		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}
		_log.log(Level.INFO, "...aggregated same as pairs in "+(System.currentTimeMillis()-time)+" ms");

		RemoteConsolidateThread[] rcts = new RemoteConsolidateThread[stubs.size()];

		_log.log(Level.INFO, "Running remote consolidation...");
		stubIter = stubs.iterator();
		for(int i=0; i<rcts.length; i++){
			rcts[i] = new RemoteConsolidateThread(stubIter.next(), i, sai, ELS);
			rcts[i].start();
		}

		_log.info("Writing sameas stats while waiting...");
		sai.logStats(TOP_K, RAND_K);
		_log.info("Finished writing sameas stats...");


		_log.log(Level.INFO, "...awaiting remote consolidation thread return...");
		for(int i=0; i<rcts.length; i++){
			rcts[i].join();
			if(!rcts[i].successful()){
				throw rcts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" consolidated...");
		}
		_log.log(Level.INFO, "...remote consolidation consolidated.");
		idletime = RMIThreads.idleTime(rcts);
		
		int[] stats = new int[((int[])rcts[0].getResult()).length];
		
		for(int i=0; i<rcts.length; i++){
			int[] rs = (int[])rcts[i].getResult();
			for(int j=0; j<rs.length; j++){
				stats[j]+=rs[j];
			}
		}
		
		_log.info("Finished consolidation, rewritten "+stats[0]+" identifiers from "+stats[3]+" possible ("+(double)stats[0]/(double)stats[3]+") with "+stats[6]+" ids in some equivalence class.");
		_log.info("Rewritten "+stats[1]+" subject identifiers from "+stats[4]+" possible ("+(double)stats[1]/(double)stats[4]+") with "+stats[7]+" ids in some equivalence class..");
		_log.info("Rewritten "+stats[2]+" object identifiers from "+stats[5]+" possible ("+(double)stats[2]/(double)stats[5]+") with "+stats[8]+" ids in some equivalence class..");
		
		_log.info("Total idle time for co-ordination on consolidation "+idletime+"...");
		_log.info("Average idle time for co-ordination on consolidation "+(double)idletime/(double)(rcts.length)+"...");

		rmic.clear();
		
		_log.log(Level.INFO, "...distributed consolidation finished.");
	}

	public static SameAsIndex buildSameAsIndex(Callback cb,  Iterator<Node[]>[] in, Iterator<Node[]> ranks){
		boolean done = false;
		SameAsIndex sai = new SameAsIndex();
		
		int count = 0;
		while(!done){
			done = true;
			for(Iterator<Node[]> i:in){
				if(i.hasNext()){
					done = false;
					Node[] next = i.next();
					cb.processStatement(next);
					sai.addSameAs(next[0], next[1]);
					count++;
				}
			}
		}
		_log.info("...exhausted iterators... read "+count+".");
		
		_log.info("...reading ranks...");
		count = 0;
		if(ranks!=null) while(ranks.hasNext()){
			sai.setRank(ranks.next());
			count++;
		}
		_log.info("...read "+count+" ranks...");
		
		return sai;
	}
	
	public static class ResetableCollectionNodeArrayIterator implements ResetableIterator<Node[]>{
		Collection<Node[]> _coll;
		Iterator<Node[]> _iter;

		public ResetableCollectionNodeArrayIterator(Collection<Node[]> coll){
			_coll = coll;
			_iter = coll.iterator();
		}

		public void reset() {
			_iter = _coll.iterator();
		}

		public boolean hasNext() {
			return _iter.hasNext();
		}

		public Node[] next() {
			return _iter.next();
		}

		public void remove() {
			_iter.remove();	
		}
	}
}
