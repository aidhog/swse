package org.semanticweb.swse.saor.master;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.engine.unique.UniqueStatementFilter;
import org.semanticweb.saorr.index.MapTripleStore;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.saorr.rules.explain.ExplainedResults;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.saor.RMIReasonerInterface;
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
public class MasterReasoner implements Master<MasterReasonerArgs>{
	private final static Logger _log = Logger.getLogger(MasterReasoner.class.getSimpleName());
	public static final String TBOX_REASONING_FILE = "tbox.r.nq.gz";
	public static final String TBOX_FILE = "tbox.nq.gz";

	public MasterReasoner(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterReasonerArgs mra) throws Exception{
		RMIClient<RMIReasonerInterface> rmic = new RMIClient<RMIReasonerInterface>(servers, stubName);
		RMIUtils.setLogFile(mra.getMasterLog());

		_log.log(Level.INFO, "Setting up remote reasoning job with following args:");
		_log.log(Level.INFO, mra.toString());

		Collection<RMIReasonerInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote reasoning...");
		Iterator<RMIReasonerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteReasonerInitThread(stubIter.next(), i, servers, mra.getSlaveArgs(i));
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
		_log.log(Level.INFO, "...remote reasoning initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Extracting tbox...");

		stubIter = stubs.iterator();
		RemoteReasonerTboxThread[] rrrts = new RemoteReasonerTboxThread[stubs.size()];

		for(int i=0; i<rrrts.length; i++){
			rrrts[i] = new RemoteReasonerTboxThread(stubIter.next(), i);
			rrrts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		InputStream[] iss = new InputStream[rrrts.length];
		for(int i=0; i<rrrts.length; i++){
			rrrts[i].join();
			if(!rrrts[i].successful()){
				throw rrrts[i].getException();
			}
			iss[i] = RemoteInputStreamClient.wrap(rrrts[i].getResult());
			_log.log(Level.INFO, "...tbox received from "+i+"...");
		}
		_log.log(Level.INFO, "...tbox received.");

		idletime = RMIThreads.idleTime(rrrts);
		_log.info("Total idle time for co-ordination on tbox "+idletime+"...");
		_log.info("Average idle time for co-ordination on tbox "+(double)idletime/(double)(ibts.length)+"...");


		_log.log(Level.INFO, "Locally aggregating tbox...");
		long time = System.currentTimeMillis();
		NxParser[] nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		RMIUtils.mkdirsForFile(mra.getTboxOut());
		RMIUtils.mkdirsForFile(mra.getReasonedTboxOut());

		String outfile = mra.getReasonedTboxOut();
		String tboxfile = mra.getTboxOut();

		OutputStream os = new GZIPOutputStream(new FileOutputStream(tboxfile)); 
		CallbackNxOutputStream cnqos = new CallbackNxOutputStream(os);

		int c = 0;
		for(NxParser nxp:nxps){
			while(nxp.hasNext()){
				cnqos.processStatement(nxp.next());
				c++;
			}
		}
		_log.info("Cached "+c+" tbox statements...");
		os.close();

		NxaGzInput tboxIn = new NxaGzInput(new File(tboxfile));

		Rules rs = new Rules(mra.getRules());
		rs.setAuthoritative();

		os = new GZIPOutputStream(new FileOutputStream(outfile)); 
		cnqos = new CallbackNxOutputStream(os);

		LinkedRuleIndex<Rule> tmplRules = buildTbox(tboxIn, rs, cnqos);
		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}
		os.close();
		_log.log(Level.INFO, "...aggregated tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");


		_log.log(Level.INFO, "Running remote reasoning...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteReasonerThread(stubIter.next(), i, tmplRules);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" reasoned...");
		}
		_log.log(Level.INFO, "...remote reasoning done.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on reasoning "+idletime+"...");
		_log.info("Average idle time for co-ordination on reasoning "+(double)idletime/(double)(ibts.length)+"...");

		rmic.clear();

		_log.log(Level.INFO, "...distributed reasoning finished.");
	}

	public static LinkedRuleIndex<Rule> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules, Callback tboxout) throws Exception{
		return buildTbox(tboxin, rules, tboxout, true, false);
	}

	public static LinkedRuleIndex<Rule> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules, Callback tboxout, boolean auth) throws Exception{
		return buildTbox(tboxin, rules, tboxout, auth, false);
	}

	public static LinkedRuleIndex<Rule> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules, Callback tboxout, boolean auth, boolean sat) throws Exception{
		if(auth){
			rules.setAuthoritative();
		}

		SortedRuleSet<Rule> all = Rules.toSet(rules.getRulesArray());
//		all.addAll(Rules.toSet(rules.getAxiomaticRules()));

		ReasonerSettings rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(auth);
		rs.setFragment(all);
		rs.setMergeRules(true);
		rs.setPrintContexts(true);
		rs.setSaturateRules(sat);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);

		ReasonerEnvironment re = new ReasonerEnvironment(null, tboxin, tboxout);

		Reasoner r = new Reasoner(rs, re);
		r.reason();

		return re.getAboxRuleIndex();

		//		int o = 0;
		//		for(Statement axTriple:rules.getAxiomaticTriples()){
		//			o++;
		//			tboxout.processStatement(axTriple.toNodeArray());
		//		}
		//		
		//		Rule[] tboxOnly = rules.getTboxOnlyRules();
		//		
		//		MapTripleStore mts = new MapTripleStore();
		//
		//		UniqueStatementFilter usf = new UniqueTripleFilter(new UniquingHashset());
		//		while(tboxin.hasNext()){
		//			Node[] next = tboxin.next();
		//			TreeSet<Statement> inferred = new TreeSet<Statement>();
		//			Statement stmt = new Statement(next);
		//			usf.addSeen(stmt);
		//			reasonTboxStatement(stmt, tboxOnly, usf, mts, inferred);
		//			
		//			for(Statement na:inferred){
		//				o++;
		//				tboxout.processStatement(na.toNodeArray());
		//			}
		//		}
		//		
		//		_log.info("Found "+o+" statements from tbox/axiomatic reasoning...");
		//		
		//		os.close();
		//		is.close();
		//		
		//		is = new GZIPInputStream(new FileInputStream(cachefile));
		//		nxp = new NxParser(is);
		//		
		//		Rule[] aboxRules = rules.getAboxRules();
		//
		//		mts = new MapTripleStore();
		//
		//		while(nxp.hasNext()){
		//			Node[] next = nxp.next();
		//			Statement stmt = new Statement(next);
		//			ai.transformStatement(stmt);
		//			for(Rule r:aboxRules){
		//				try {
		//					r.handleTBoxStatement(mts, stmt);
		//				} catch (InconsistencyException e) {
		//					;
		//				}
		//			}
		//		}
		//		
		//		_log.info("Finished with "+c+" tbox statements...");
		//		return mts;
	}

	public static void reasonTboxStatement(Statement todo, Rule[] tboxOnly, UniqueStatementFilter usf, MapTripleStore mts, TreeSet<Statement> done){
		for(Rule r:tboxOnly){
			try {
				ExplainedResults er = r.handleTBoxStatement(mts, todo);
				if(er!=null && er.getAllInferences()!=null) for(Statement na:er.getAllInferences()){
					if(!usf.addSeen(na) && done.add(na)){
						reasonTboxStatement(todo, tboxOnly, usf, mts, done);
					}
				}
			} catch (InconsistencyException e) {
				;
			}
		}
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

