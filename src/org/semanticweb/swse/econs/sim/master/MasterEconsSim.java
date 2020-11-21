package org.semanticweb.swse.econs.sim.master;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.semanticweb.swse.econs.sim.RMIEconsSimConstants;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer;
import org.semanticweb.swse.econs.sim.SlaveEconsSimArgs;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.swse.econs.sim.utils.Stats;
import org.semanticweb.yars.nx.Node;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterEconsSim implements Master<MasterEconsSimArgs>{
	private final static Logger _log = Logger.getLogger(MasterEconsSim.class.getSimpleName());
	public static final int TICKS = 10000000;

	public static final String PRED_STATS = "pred.stats.jo.gz";
	public static final String ALL_STATS = "final.stats.jo.gz";

	public MasterEconsSim(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterEconsSimArgs msa) throws Exception{
		RMIClient<RMIEconsSimInterface> rmic = new RMIClient<RMIEconsSimInterface>(servers, stubName);

		RMIUtils.setLogFile(msa.getMasterLog());
		
		ArrayList<HashMap<Node,PredStats>> pStats = new ArrayList<HashMap<Node,PredStats>>();
		
		if(msa.getSkipToAgg()){
			ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(msa.getPredicateStatsFilename())));
			pStats = ArrayList.class.cast(ois.readObject());
			msa.getSlaveArgs().setPredStats(pStats);
		} 

		_log.log(Level.INFO, "Setting up remote sim job with following args:");
		_log.log(Level.INFO, msa.toString());

		Collection<RMIEconsSimInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote similarity...");
		Iterator<RMIEconsSimInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteSimInitThread(stubIter.next(), i, servers, msa.getSlaveArgs(i), RMIEconsSimConstants.DEFAULT_STUB_NAME);
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

		

		if(!msa.getSkipToAgg()){
			_log.log(Level.INFO, "Getting predicate statistics...");
			RemoteSimPredStatsThread[] rsps = new RemoteSimPredStatsThread[stubs.size()];
			stubIter = stubs.iterator();

			for(int i=0; i<rsps.length; i++){
				rsps[i] = new RemoteSimPredStatsThread(stubIter.next(), i);
				rsps[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<rsps.length; i++){
				rsps[i].join();
				if(!rsps[i].successful()){
					throw rsps[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" pred stats done...");
			}
			_log.log(Level.INFO, "...remote pred stats finished.");
			idletime = RMIThreads.idleTime(rsps);
			_log.info("Total idle time for co-ordination on pred stats "+idletime+"...");
			_log.info("Average idle time for co-ordination on pred stats "+(double)idletime/(double)(rsps.length)+"...");

			_log.log(Level.INFO, "Locally aggregating predicate statistics...");
			long b4 = System.currentTimeMillis();
			ArrayList<HashMap<Node,PredStats>> allPredStats = new ArrayList<HashMap<Node,PredStats>>();

			for(int i=0; i<rsps.length; i++){
				allPredStats.add(rsps[i].getResult().get(0));
			}

			HashMap<Node,PredStats> psStats = mergeAndAdjust(allPredStats);

			allPredStats = new ArrayList<HashMap<Node,PredStats>>();

			for(int i=0; i<rsps.length; i++){
				allPredStats.add(rsps[i].getResult().get(1));
			}

			HashMap<Node,PredStats> poStats = mergeAndAdjust(allPredStats);

			allPredStats = null;
			rsps = null;

			pStats.add(psStats);
			pStats.add(poStats);
			_log.info("...done local p-stat aggregation in "+(System.currentTimeMillis()-b4)+" ms");

			_log.info("...saving to "+PRED_STATS+"...");
			ObjectOutputStream os = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(PRED_STATS)));
			os.writeObject(pStats);
			os.close();
			_log.info("...saved to "+PRED_STATS+"...");

			_log.log(Level.INFO, "Generating similarity tuples...");
			RemoteGenerateSimThread[] rgss = new RemoteGenerateSimThread[stubs.size()];
			stubIter = stubs.iterator();

			for(int i=0; i<rgss.length; i++){
				rgss[i] = new RemoteGenerateSimThread(stubIter.next(), i, pStats);
				rgss[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<rgss.length; i++){
				rgss[i].join();
				if(!rgss[i].successful()){
					throw rgss[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" sim generated...");
			}
			_log.log(Level.INFO, "...remote sim generation finished.");
			idletime = RMIThreads.idleTime(rgss);
			_log.info("Total idle time for co-ordination on sim generation "+idletime+"...");
			_log.info("Average idle time for co-ordination on sim generation "+(double)idletime/(double)(rgss.length)+"...");

			int sp = 0, op = 0;
			for(int i=0; i<rgss.length; i++){
				sp += rgss[i].getResult().get(0);
				op += rgss[i].getResult().get(1);
			}

			_log.info("Written "+sp+" tuples total from SPs");
			_log.info("Written "+op+" tuples total from OPs");

			_log.log(Level.INFO, "Scattering similarity tuples...");
			RemoteScatterSimThread[] rsss = new RemoteScatterSimThread[stubs.size()];
			stubIter = stubs.iterator();

			for(int i=0; i<rsss.length; i++){
				rsss[i] = new RemoteScatterSimThread(stubIter.next(), i);
				rsss[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<rsss.length; i++){
				rsss[i].join();
				if(!rsss[i].successful()){
					throw rsss[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" scattered...");
			}
			_log.log(Level.INFO, "...remote scatter finished.");
			idletime = RMIThreads.idleTime(rsss);
			_log.info("Total idle time for co-ordination on scatter "+idletime+"...");
			_log.info("Average idle time for co-ordination on scatter "+(double)idletime/(double)(rsss.length)+"...");
		}

		_log.log(Level.INFO, "Aggregating similarity tuples...");
		RemoteAggregateSimThread[] rass = new RemoteAggregateSimThread[stubs.size()];
		stubIter = stubs.iterator();

		for(int i=0; i<rass.length; i++){
			rass[i] = new RemoteAggregateSimThread(stubIter.next(), i);
			rass[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rass.length; i++){
			rass[i].join();
			if(!rass[i].successful()){
				throw rass[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" sim generated...");
		}
		_log.log(Level.INFO, "...remote sim generation finished.");
		idletime = RMIThreads.idleTime(rass);
		_log.info("Total idle time for co-ordination on sim aggregation "+idletime+"...");
		_log.info("Average idle time for co-ordination on sim aggregation "+(double)idletime/(double)(rass.length)+"...");

		Stats<Double> all = new Stats<Double>(SlaveEconsSimArgs.TOP_K, SlaveEconsSimArgs.RAND_K);
		for(int i=0; i<rass.length; i++){
			all.addAll(rass[i].getResult());
		}

		all.logStats();

		_log.info("...saving to "+ALL_STATS+"...");
		ObjectOutputStream os = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(ALL_STATS)));
		os.writeObject(all);
		os.close();
		_log.info("...saved to "+ALL_STATS+"...");

		_log.info("...finished.");
	}

	public static HashMap<Node,PredStats> mergeAndAdjust(ArrayList<HashMap<Node,PredStats>> predStats){
		HashMap<Node,PredStats> merged = new HashMap<Node,PredStats>();


		for(HashMap<Node,PredStats> predStat:predStats){
			for(Map.Entry<Node, PredStats> ps:predStat.entrySet()){
				//				if(ps.getKey().equals(RDFS.ISDEFINEDBY)){
				//					System.err.println(ps);
				//				}
				PredStats old = merged.get(ps.getKey());
				if(old==null){
					merged.put(ps.getKey(), ps.getValue());
				} else{
					old.addPredStats(ps.getValue());
				}
			}
		}

		PredStats all = merged.get(RMIEconsSimServer.ALL);
		double averageOccur = all.getOccurrenceCount()/(double)merged.size();
		double averageTriple = all.getTriples()/(double)merged.size();

		_log.info("All occur "+all.getOccurrenceCount());
		_log.info("All triples "+all.getTriples());
		_log.info("Merged size "+merged.size());
		_log.info("Average occur "+averageOccur);
		_log.info("Average triple "+averageTriple);
		_log.info("Average overall "+averageTriple/averageOccur);

		for(Map.Entry<Node, PredStats> ps:merged.entrySet()){
			_log.info("@ Pre-adjusted "+ps.getKey().toN3()+" "+ps.getValue());
			ps.getValue().adjust(averageOccur, averageTriple);
			_log.info("@ Adjusted "+ps.getKey().toN3()+" "+ps.getValue());
		}

		return merged;
	}

}
