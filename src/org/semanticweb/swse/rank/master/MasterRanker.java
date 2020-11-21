package org.semanticweb.swse.rank.master;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.deri.idrank.pagerank.PageRank;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.rank.RMIRankingInterface;
import org.semanticweb.swse.rank.utils.CallbackPageRank;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterRanker implements Master<MasterRankingArgs>{
	public static final String RANKS_FILE = "ranks.nx.gz";
	private final static Logger _log = Logger.getLogger(MasterRanker.class.getSimpleName());


	public MasterRanker(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterRankingArgs mra) throws Exception{
		RMIClient<RMIRankingInterface> rmic = new RMIClient<RMIRankingInterface>(servers, stubName);
		
		RMIUtils.setLogFile(mra.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote ranking job with following args:");
		_log.log(Level.INFO, mra.toString());
		
		Collection<RMIRankingInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		RMIUtils.mkdirsForFile(mra.getLocalRanksOut());
		
		_log.log(Level.INFO, "Initialising remote ranking...");
		Iterator<RMIRankingInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteRankInitThread(stubIter.next(), i, servers, mra.getSlaveArgs(i));
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

		_log.log(Level.INFO, "Getting named authority ranks...");

		stubIter = stubs.iterator();
		RemoteRankNameAuthThread[] rrnas = new RemoteRankNameAuthThread[stubs.size()];
		for(int i=0; i<rrnas.length; i++){
			rrnas[i] = new RemoteRankNameAuthThread(stubIter.next(), i);
			rrnas[i].start();
		}

		TreeSet<Node[]> naranks = new TreeSet<Node[]>(NodeComparator.NC);
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rrnas.length; i++){
			rrnas[i].join();
			if(!rrnas[i].successful()){
				throw rrnas[i].getException();
			}
			naranks.addAll(rrnas[i].getResult());
			_log.log(Level.INFO, "...named authority received from "+i+"...");
		}
		_log.log(Level.INFO, "..."+naranks.size()+" named authority links received.");

		idletime = RMIThreads.idleTime(rrnas);
		_log.info("Total idle time for co-ordination on getting named authority "+idletime+"...");
		_log.info("Average idle time for co-ordination on getting named authority "+(double)idletime/(double)(ibts.length)+"...");


		_log.log(Level.INFO, "Locally calculating PageRank of named authority...");
		long time = System.currentTimeMillis();
		PageRank pr = new PageRank(new ResetableCollectionNodeArrayIterator(naranks), mra.getIterations(), mra.getDamping());
		CallbackPageRank cpr = new CallbackPageRank();
		pr.process(cpr);
		_log.info("...pagerank calculated in "+(System.currentTimeMillis()-time)+" ms");
		_log.log(Level.INFO, "...calculated PageRank of named authority.");

		for(Map.Entry<String, Float> rank:cpr.getMap().entrySet()){
			_log.fine("NA Rank: "+rank.getKey()+" "+rank.getValue());
		}

		_log.info("Total PLDs ranked: "+cpr.getMap().size());

		Count<Node> indegree = new Count<Node>();
		Count<Node> outdegree = new Count<Node>();
		for(Node[] na:naranks){
			_log.fine("NA-Link: "+na[0].toN3()+" "+cpr.getMap().get(na[0].toString())+" "+na[1].toN3()+" "+cpr.getMap().get(na[1].toString()));
			outdegree.add(na[0]);
			indegree.add(na[1]);
		}

		_log.info("Total inlinks: "+indegree.getTotal()+". Total inlinkers: "+indegree.size()+" Average inlinks: "+(double)indegree.getTotal()/(double)indegree.size());
		for(Map.Entry<Node,Integer> in:indegree.entrySet()){
			_log.fine("NA-Indegree: "+in.getKey().toN3()+" "+in.getValue());
		}

		_log.info("Total outlinks: "+outdegree.getTotal()+". Total outlinkers: "+outdegree.size()+" Average outlinks: "+(double)outdegree.getTotal()/(double)outdegree.size());	
		for(Map.Entry<Node,Integer> out:outdegree.entrySet()){
			_log.fine("NA-Outdegree: "+out.getKey().toN3()+" "+out.getValue());
		}

		_log.log(Level.INFO, "Calculating identifier ranks...");
		stubIter = stubs.iterator();
		RemoteRankIDThread[] rris = new RemoteRankIDThread[stubs.size()];
		for(int i=0; i<rris.length; i++){
			rris[i] = new RemoteRankIDThread(stubIter.next(), i, cpr.getMap());
			rris[i].start();
		}

		InputStream[] iss = new InputStream[stubs.size()];
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rris.length; i++){
			rris[i].join();
			if(!rris[i].successful()){
				throw rris[i].getException();
			}
			iss[i] = RemoteInputStreamClient.wrap(rris[i].getResult());
			_log.log(Level.INFO, "...identifier ranks done for "+i+"...");
		}
		_log.log(Level.INFO, "...identifier ranks calculated.");

		idletime = RMIThreads.idleTime(rris);
		_log.info("Total idle time for co-ordination on getting identifier ranks "+idletime+"...");
		_log.info("Average idle time for co-ordination on getting identifier ranks "+(double)idletime/(double)(rris.length)+"...");


		_log.log(Level.INFO, "Locally aggregating ranks for identifiers...");
		NxParser nxp[] = new NxParser[iss.length];
		for(int i=0; i<nxp.length; i++){
			nxp[i] = new NxParser(new GZIPInputStream(iss[i]));
		}
		MergeSortIterator si = new MergeSortIterator(nxp);
		RMIUtils.mkdirsForFile(mra.getLocalRanksOut());

		OutputStream os = new FileOutputStream(mra.getLocalRanksOut());
		if(mra.getGzRanksOut())
			os = new GZIPOutputStream(os);

		aggregateRanks(si, new CallbackNxOutputStream(os));
		os.close();
		for(InputStream is:iss){
			is.close();
		}
		_log.log(Level.INFO, "...aggregated ranks for identifiers.");

		if(mra.getSlaveArgs(0).getOutFinal()!=null){
			_log.log(Level.INFO, "Copying final ranks to remote servers...");
			iss = new InputStream[ibts.length];
			RemoteInputStreamServer[] istream = new RemoteInputStreamServer[ibts.length];
			for(int i=0; i<ibts.length; i++){
				try {
					iss[i] = new BufferedInputStream(
							new FileInputStream(mra.getLocalRanksOut()));
					istream[i] = new SimpleRemoteInputStream(iss[i]);
					RemoteInputStream ris = istream[i].export();

					_log.info("Flooding "+mra.getLocalRanksOut()+" to remote server");
					RMIRankingInterface rmri = rmic.getStub(i);

					ibts[i] = new RemoteRankGatherRanksThread(rmri, i, ris);

					ibts[i].start();
				} catch(IOException e){ 
					_log.log(Level.SEVERE, "Error creating RemoteInputStream on master server\n"+e);
					e.printStackTrace();
					throw new RemoteException("Error creating RemoteInputStream on master server\n"+e);
				}
			}
			
			_log.info("..awaiting thread return for flooding ranks...");
			
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				iss[i].close();
				istream[i].close();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" sent...");
			}
			_log.info("..ranks flooded.");
		}
		
		rmic.clear();
		
		_log.log(Level.INFO, "...distributed ranking finished.");
	}

	private static void aggregateRanks(Iterator<Node[]> in, Callback out){
		long time = System.currentTimeMillis();
		Node olds = null, oldc = null, oldr = null;
		Node[] next;
		float sum = 0;
		while(in.hasNext()){
			next = in.next();
			if(olds!=null && !next[0].equals(olds)){
				out.processStatement(new Node[]{olds, new Literal(Float.toString(sum))});
				sum = 0;
			}
			if(olds!=null && olds.equals(next[0]) && oldc!=null && oldc.equals(next[1])){
				_log.warning("Repetition of context "+next[1]+" for URI "+next[0]+" oldr "+oldr.toN3()+" newr "+next[2].toN3());
			}
			olds = next[0];
			oldc = next[1];
			oldr = next[2];
			sum += Float.parseFloat(next[2].toString());
		}
		out.processStatement(new Node[]{olds, new Literal(Double.toString(sum))});

		long time2 = System.currentTimeMillis();
		_log.info("...aggregating ranks finished in "+(time2-time)+" ms");
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
