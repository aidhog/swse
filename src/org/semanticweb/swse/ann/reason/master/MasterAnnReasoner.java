package org.semanticweb.swse.ann.reason.master;

import java.io.File;
import java.io.FileInputStream;
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

import org.semanticweb.saorr.ann.AnnotatedStatement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.engine.unique.UniqueAnnotatedStatementFilter;
import org.semanticweb.saorr.ann.index.AnnotatedMapTripleStore;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.ann.rules.explain.AnnotatedExplainedResults;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.LinkedRuleIndex.LinkedRule;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.utils.AggregateTripleRanksIterator;
import org.semanticweb.swse.ann.rank.utils.RankTriplesIterator;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerInterface;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterAnnReasoner implements Master<MasterAnnReasonerArgs>{
	private final static Logger _log = Logger.getLogger(MasterAnnReasoner.class.getSimpleName());
	public static final String TBOX_REASONING_FILE = "tbox.r.nq.gz";
	public static final String TBOX_FILE = "tbox.nq.gz";
	
	public MasterAnnReasoner(){
		;
	}
	
	public void startRemoteTask(RMIRegistries servers, String stubName, MasterAnnReasonerArgs mra) throws Exception{
		RMIClient<RMIAnnReasonerInterface> rmic = new RMIClient<RMIAnnReasonerInterface>(servers, stubName);
		RMIUtils.setLogFile(mra.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote reasoning job with following args:");
		_log.log(Level.INFO, mra.toString());
		
		Collection<RMIAnnReasonerInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote reasoning...");
		Iterator<RMIAnnReasonerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAnnReasonerInitThread(stubIter.next(), i, servers, mra.getSlaveArgs(i));
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
		RemoteAnnReasonerTboxThread[] rrrts = new RemoteAnnReasonerTboxThread[stubs.size()];
		
		for(int i=0; i<rrrts.length; i++){
			rrrts[i] = new RemoteAnnReasonerTboxThread(stubIter.next(), i);
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
		
		
		NodeComparatorArgs nca = new NodeComparatorArgs();
		nca.setOrder(new int[]{3,0,1,2});
		MergeSortArgs msa = new MergeSortArgs(nxps);
		msa.setComparator(new NodeComparator(nca));
		MergeSortIterator msi = new MergeSortIterator(msa);
		
//		while(msi.hasNext()){
//			System.err.println(Nodes.toN3(msi.next()));
//			
//		}
//		
//		if(true)
//			System.exit(0);
		
		_log.log(Level.INFO, "...locally ranking tbox...");
		InputStream is = new FileInputStream(mra.getRanksIn());
		if(mra.getGzRanksIn())
			is = new GZIPInputStream(is);
		NxParser ranks = new NxParser(is);
		
		RankTriplesIterator rti = new RankTriplesIterator(msi, ranks);	
		
		SortIterator si = new SortIterator(rti);
		AggregateTripleRanksIterator atri = new AggregateTripleRanksIterator(si);
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(tboxfile)); 
		CallbackNxOutputStream cnqos = new CallbackNxOutputStream(os);
		
		int c = 0;
		while(atri.hasNext()){
			cnqos.processStatement(atri.next());
			c++;
		}
		_log.info("...cached/merge-sorted/sorted/ranked "+c+" tbox statements in "+(System.currentTimeMillis()-time)+" ms");
		os.close();
		is.close();
		
		for(InputStream ris:iss){
			ris.close();
		}
		
		FileInput tboxIn = null;
		if(mra.getAuth()) tboxIn = new NxaGzInput(new File(tboxfile),4);
		else tboxIn = new NxGzInput(new File(tboxfile));
		
		ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);
		
		Rules rs = new Rules(mra.getRules());
		if(mra.getAuth()) rs.setAuthoritative();
		
		os = new GZIPOutputStream(new FileOutputStream(outfile)); 
		cnqos = new CallbackNxOutputStream(os);
		
		LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules = buildTbox(fwiter, rs, cnqos, mra.getAuth());
		
		for(LinkedRule<AnnotationRule<RankAnnotation>> tmplRule : tmplRules.getAllLinkedRules()){
			_log.fine(tmplRule.getRule()+" (links "+tmplRule.linksCount()+")");
		}
		
		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}
		os.close();
		_log.log(Level.INFO, "...aggregated tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");
		
		
		_log.log(Level.INFO, "Running remote reasoning...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAnnReasonerThread(stubIter.next(), i, tmplRules);
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
	
	public static LinkedRuleIndex<AnnotationRule<RankAnnotation>> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules, Callback tboxout) throws Exception{
		return buildTbox(tboxin, rules, tboxout, true);
	}
	
	public static LinkedRuleIndex<AnnotationRule<RankAnnotation>> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules, Callback tboxout, boolean auth) throws Exception{
		ReasonerSettings rs = new ReasonerSettings();
		if(auth)
			rs.setAuthorativeReasoning(true);
		rs.setFragment(rules.getRulesArray());
		rs.setMergeRules(false);
		rs.setPrintContexts(true);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);
		
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(null, tboxin, RankAnnotationFactory.SINGLETON, tboxout);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rs, re);
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
	
	public static void reasonTboxStatement(AnnotatedStatement<RankAnnotation> todo, Collection<AnnotationRule<RankAnnotation>> tboxOnly, UniqueAnnotatedStatementFilter<RankAnnotation> usf, AnnotatedMapTripleStore<RankAnnotation> mts, TreeSet<AnnotatedStatement<RankAnnotation>> done){
		for(AnnotationRule<RankAnnotation> r:tboxOnly){
			try {
				AnnotatedExplainedResults<RankAnnotation> aer = r.handleAnnotatedTBoxStatement(mts, todo);
				if(aer.getAllInferences()!=null) for(AnnotatedStatement<RankAnnotation> na:aer.getAllInferences()){
					if(!usf.addSeen(na) && done.add(na)){
						reasonTboxStatement(todo, tboxOnly, usf, mts, done);
					}
				}
			} catch (InconsistencyException e) {
				;
			}
		}
	}
}

