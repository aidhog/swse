package org.semanticweb.swse.ann.repair.master;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.ann.AnnotatedStatement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.engine.unique.UniqueAnnotatedStatementFilter;
import org.semanticweb.saorr.ann.engine.unique.UniquingAnnotationHashset;
import org.semanticweb.saorr.ann.index.AnnotatedMapTripleStore;
import org.semanticweb.saorr.ann.index.StatementAnnotationMap;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.ann.rules.explain.AnnotatedExplainedResults;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.ih.InconsistencyLogParser;
import org.semanticweb.saorr.engine.ih.InconsistencyLogParser.InconsistencyInfo;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.swse.ann.repair.RMIAnnRepairInterface;
import org.semanticweb.swse.cons.master.MasterConsolidation.ResetableCollectionNodeArrayIterator;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.util.ResetableIterator;

import com.ontologycentral.ldspider.hooks.content.CallbackDummy;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterAnnRepair implements Master<MasterAnnRepairArgs>{
	private final static Logger _log = Logger.getLogger(MasterAnnRepair.class.getSimpleName());
	public static final String TBOX_REASONING_FILE = "tbox.r.nq.gz";
	public static final String TBOX_FILE = "tbox.nq.gz";

	public static final Rule[] DEFAULT_RULES = Fragment.getRules(WOL_T_SPLIT.class);

	public MasterAnnRepair(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterAnnRepairArgs mra) throws Exception{
		RMIClient<RMIAnnRepairInterface> rmic = new RMIClient<RMIAnnRepairInterface>(servers, stubName);
		RMIUtils.setLogFile(mra.getMasterLog());

		_log.log(Level.INFO, "Setting up remote reasoning job with following args:");
		_log.log(Level.INFO, mra.toString());

		Collection<RMIAnnRepairInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote reasoning...");
		Iterator<RMIAnnRepairInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAnnRepairInitThread(stubIter.next(), i, servers, mra.getSlaveArgs(i));
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

		_log.info("Loading local inconsistency file...");

		InconsistencyLogParser ilp = new InconsistencyLogParser(new File(mra._ininc), mra._inconGz);

		//		Map<Statement, ArrayList<RankedInconsistencyInfo>> incons = new HashMap<Statement, ArrayList<RankedInconsistencyInfo>>(); 
		StatementAnnotationMap<RankAnnotation> delta = new StatementAnnotationMap<RankAnnotation>();

		double sumrank = 0;
		while(ilp.hasNext()){
			InconsistencyInfo ii = ilp.next();
			for(int i = 0; i<ii.getData().size(); i++){
				RankedInconsistencyInfo rii = new RankedInconsistencyInfo(ii.getData().get(i), ii.getRuleAnnotation(), ii.getDataAnnotations().get(i));
				//				for(Nodes s: ii.getData().get(i)){
				//					Statement st = new Statement(s.getNodes());
				//					ArrayList<RankedInconsistencyInfo> incon = incons.get(st);
				//					if(incon == null){
				//						incon = new ArrayList<RankedInconsistencyInfo>();
				//						incons.put(st, incon);
				//					}
				//					incon.add(rii);
				//				}
				AnnotatedStatement<RankAnnotation> min = rii.getMin();
				if(delta.putAnnotation(min)){
					_log.info("Delta INIT "+min);
					sumrank+=min.getAnnotation().getRank();
				}
			}
		}

		_log.info("...loaded inconsistencies... Delta size: "+delta.size()+" Delta sum rank "+sumrank);

		_log.log(Level.INFO, "Extracting delta plus...");

		stubIter = stubs.iterator();
		RemoteAnnRepairDeltaPlusThread[] rrrts = new RemoteAnnRepairDeltaPlusThread[stubs.size()];

		for(int i=0; i<rrrts.length; i++){
			rrrts[i] = new RemoteAnnRepairDeltaPlusThread(stubIter.next(), i, delta);
			rrrts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rrrts.length; i++){
			rrrts[i].join();
			if(!rrrts[i].successful()){
				throw rrrts[i].getException();
			}
			_log.log(Level.INFO, "...delta plus received from "+i+"...");
		}
		_log.log(Level.INFO, "...delta plus received.");

		idletime = RMIThreads.idleTime(rrrts);
		_log.info("Total idle time for co-ordination on tbox "+idletime+"...");
		_log.info("Average idle time for co-ordination on tbox "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating new delta...");

		sumrank = 0;
		for(RemoteAnnRepairDeltaPlusThread rrrt:rrrts){
			for(Map.Entry<Statement,RankAnnotation> e:rrrt.getResult().entrySet()){
				AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
				delta.putAnnotation(as);
				_log.info("DeltaNew "+as);
				sumrank++;
			}
		}

		rrrts = null;
		_log.log(Level.INFO, "...done.");

		_log.info("...loaded inconsistencies... New delta size: "+delta.size()+" delta sum rank "+sumrank);

		_log.info("Loading tbox...");
		long time = System.currentTimeMillis();
		FileInput tboxIn = null;
		if(mra._auth) tboxIn = new NxaGzInput(new File(mra._intbox),4);
		else tboxIn = new NxGzInput(new File(mra._intbox));

		ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);

		Rules rs = new Rules(DEFAULT_RULES);
		Rules abox = new Rules(rs.getAboxRules());
		if(mra._auth) abox.setAuthoritative();

		CallbackDummy cb = new CallbackDummy();

		LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules = buildTbox(fwiter, abox, cb, mra._auth);
		tmplRules.freeResources();

		tboxIn.close();
		_log.log(Level.INFO, "...loaded tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");


		ResetableCollectionNodeArrayIterator coll = new ResetableCollectionNodeArrayIterator(new ArrayList<Node[]>());

		StatementAnnotationMap<RankAnnotation> deltaPlus = new StatementAnnotationMap<RankAnnotation>();

		ReasonerSettings rset = new ReasonerSettings();
		rset.setSkipTBox(true);
		rset.setSkipAxiomatic(true);
		rset.setPrintContexts(true);

		CallbackDummy cs = new CallbackDummy();
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(coll, RankAnnotationFactory.SINGLETON, cs);
		re.setAboxRuleIndex(tmplRules);
		rset.setUseAboxRuleIndex(true);

		UniqueAnnotatedStatementFilter<RankAnnotation> uasf = new UniquingAnnotationHashset<RankAnnotation>();
		re.setUniqueStatementFilter(uasf);

		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rset, re);

		SortedRuleSet<AnnotationRule<RankAnnotation>> aboxRules = new SortedRuleSet<AnnotationRule<RankAnnotation>>();

		sumrank = 0;
		int schemaSkipped = 0;
		for(Map.Entry<Statement,RankAnnotation> e:delta.entrySet()){
			AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
			HashSet<AnnotatedStatement<RankAnnotation>> set = new HashSet<AnnotatedStatement<RankAnnotation>>();

			Set<Node> data = getDataNodes(as);

			try{
				r.reasonAbox(as, aboxRules, set);
			} catch(InconsistencyException ie){
				_log.severe(ie.getMessage());
			}

			for(AnnotatedStatement<RankAnnotation> s:set){
				if(intersects(s, data)){
					if(deltaPlus.putAnnotation(s)){
						_log.info("Delta+ "+s);
						sumrank += s.getAnnotation().getRank();;
					}
				} else{
					schemaSkipped++;
				}
			}
		}

		_log.info("delta plus "+deltaPlus.size()+" sumrank "+sumrank+" schema skipped "+schemaSkipped);


		_log.log(Level.INFO, "Extracting delta minus/d...");

		stubIter = stubs.iterator();
		RemoteAnnRepairDeltaDRThread[] rrrdts = new RemoteAnnRepairDeltaDRThread[stubs.size()];

		for(int i=0; i<rrrdts.length; i++){
			rrrdts[i] = new RemoteAnnRepairDeltaDRThread(stubIter.next(), i, delta, deltaPlus);
			rrrdts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rrrdts.length; i++){
			rrrdts[i].join();
			if(!rrrdts[i].successful()){
				throw rrrdts[i].getException();
			}
			_log.log(Level.INFO, "...delta dr received from "+i+"...");
		}
		_log.log(Level.INFO, "...delta dr received.");

		idletime = RMIThreads.idleTime(rrrdts);
		_log.info("Total idle time for co-ordination on delta dr "+idletime+"...");
		_log.info("Average idle time for co-ordination on delta dr "+(double)idletime/(double)(rrrdts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating delta drs...");

		StatementAnnotationMap<RankAnnotation> deltaD = new StatementAnnotationMap<RankAnnotation>();
		for(RemoteAnnRepairDeltaDRThread rrrt:rrrdts){
			for(Map.Entry<Statement,RankAnnotation> e:rrrt.getResult().get(1).entrySet()){
				AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
				deltaD.putAnnotation(as);
				_log.info("DeltaD Init "+as);
			}
		}

		for(RemoteAnnRepairDeltaDRThread rrrt:rrrdts){
			for(Map.Entry<Statement,RankAnnotation> e:rrrt.getResult().get(0).entrySet()){
				AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
				deltaPlus.removeAnnotation(as);
				deltaD.removeAnnotation(as);
				_log.info("Delta- "+as);
			}
		}
		
		rrrdts = null;

		//		System.err.println("d+     "+deltaPlus.entrySet());
		//		System.err.println("dD     "+deltaD.entrySet());

		sumrank = 0;

		for(Map.Entry<Statement,RankAnnotation> as:deltaPlus.entrySet()){
			sumrank+=as.getValue().getRank();
		}

		double sumrank2 = 0;

		for(Map.Entry<Statement,RankAnnotation> e:deltaD.entrySet()){
			AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
			sumrank2+=as.getAnnotation().getRank();
			_log.info("DeltaDF "+as);
		}

		_log.log(Level.INFO, "...done. Delta plus new size "+deltaPlus.size()+" new rank "+sumrank+" delta d size "+deltaD.size()+" delta d sum rank "+sumrank2);

		double sumrank3 = 0;
		for(Map.Entry<Statement,RankAnnotation> e:deltaPlus.entrySet()){
			AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
			sumrank3+=as.getAnnotation().getRank();
			_log.info("DeltaAdd "+as);
			delta.putAnnotation(as);
		}

		double sumrank4 = 0;

		for(Map.Entry<Statement,RankAnnotation> e:delta.entrySet()){
			AnnotatedStatement<RankAnnotation> as = new AnnotatedStatement<RankAnnotation>(e.getKey(), e.getValue());
			sumrank4+=as.getAnnotation().getRank();
			_log.info("DeltaF "+as);
		}

		_log.log(Level.INFO, "...done. Delta final plus size "+deltaPlus.size()+" new rank "+sumrank3);
		_log.log(Level.INFO, "...done. Delta final size "+delta.size()+" new rank "+sumrank4);
		
		deltaPlus = null;

		_log.log(Level.INFO, "Repairing...");

		stubIter = stubs.iterator();
		RemoteAnnRepairThread[] rarts = new RemoteAnnRepairThread[stubs.size()];

		for(int i=0; i<rarts.length; i++){
			rarts[i] = new RemoteAnnRepairThread(stubIter.next(), i, delta, deltaD);
			rarts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<rarts.length; i++){
			rarts[i].join();
			if(!rarts[i].successful()){
				throw rarts[i].getException();
			}
			_log.log(Level.INFO, "...repair done on "+i+"...");
		}
		_log.log(Level.INFO, "...repair done.");

		idletime = RMIThreads.idleTime(rarts);
		_log.info("Total idle time repairing "+idletime+"...");
		_log.info("Average idle time repairing "+(double)idletime/(double)(rarts.length)+"...");

		rmic.clear();

		_log.log(Level.INFO, "...distributed repair finished.");
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

	public static class RankedInconsistencyInfo {//implements Comparable<RankedInconsistencyInfo>{
		private RankAnnotation _vD = null;
		private RankAnnotation _cD = null;
		private TreeSet<AnnotatedStatement<RankAnnotation>> _data;
		private TreeSet<RankAnnotation> _anns;

		public RankedInconsistencyInfo(Set<Nodes> data, Nodes cD, Nodes vD){
			RankAnnotationFactory raf = RankAnnotationFactory.SINGLETON;
			_data = new TreeSet<AnnotatedStatement<RankAnnotation>>();
			_anns = new TreeSet<RankAnnotation>();
			_vD = new RankAnnotation(Double.parseDouble(vD.getNodes()[0].toString()));
			_cD = new RankAnnotation(Double.parseDouble(cD.getNodes()[0].toString()));
			for(Nodes ns:data){
				_data.add(raf.fromNodes(ns.getNodes()));
			}
		}

		public RankedInconsistencyInfo(Set<Nodes> data, RankAnnotation cD, RankAnnotation vD){
			RankAnnotationFactory raf = RankAnnotationFactory.SINGLETON;
			_data = new TreeSet<AnnotatedStatement<RankAnnotation>>();
			_anns = new TreeSet<RankAnnotation>();
			_vD = vD;
			_cD = cD;
			for(Nodes ns:data){
				_data.add(raf.fromNodes(ns.getNodes()));
			}
		}

		public RankAnnotation getRuleAnnotation(){
			return _cD;
		}

		public RankAnnotation getViolationDegree(){
			return _vD;
		}

		public TreeSet<RankAnnotation> getDataAnnotations(){
			return _anns;
		}

		public TreeSet<AnnotatedStatement<RankAnnotation>> getData(){
			return _data;
		}

		public AnnotatedStatement<RankAnnotation> getMin(){
			for(AnnotatedStatement<RankAnnotation> min: _data){
				if(min.getAnnotation().equals(_vD)){
					return min;
				}
			}
			return _data.first();
		}


		//	public int compareTo(RankedInconsistencyInfo o) {
		//		if(o==this)
		//			return 0;
		//		if(o._data.size()<=1 && _data.size()>1){
		//			return -1;
		//		} else if(o._data.size()>1 && o._data.size()<=1){
		//			return 1;
		//		} else{
		//			TreeSet<RankAnnotation> degreeSet1 = new TreeSet<RankAnnotation>();
		//			TreeSet<RankAnnotation> degreeSet2 = new TreeSet<RankAnnotation>();
		//		}
		//	}
	}

	//	public static class RankedInconsistencyInfo {//implements Comparable<RankedInconsistencyInfo>{
	//		private String _rule_id = null;
	//		private RankAnnotation _rule_ann = null;
	//		private ArrayList<RankAnnotation> _data_anns = null;
	//		private ArrayList<Set<Nodes>> _data = null;
	//		TreeSet<RankAnnotation> degreeSet = null;
	//
	//		public RankedInconsistencyInfo(InconsistencyInfo ii){
	//			_data = ii.getData();
	//			_rule_ann = new RankAnnotation(Double.parseDouble(ii.getRuleAnnotation().getNodes()[0].toString()));
	//			_data_anns = new ArrayList<RankAnnotation>(ii.getDataAnnotations().size());
	//			for(Nodes da:ii.getDataAnnotations()){
	//				_data_anns.add(new RankAnnotation(Double.parseDouble(da.getNodes()[0].toString())));
	//			}
	//		}
	//
	//		public String getRuleId(){
	//			return _rule_id;
	//		}
	//
	//		public RankAnnotation getRuleAnnotation(){
	//			return _rule_ann;
	//		}
	//
	//		public RankAnnotation getMinDegree(){
	//			RankAnnotation ra = _rule_ann.clone();
	//
	//			for(int i=0; i<_data_anns.size(); i++){
	//				ra = ra.meet(_data_anns.get(i));
	//			}
	//
	//			return ra;
	//		}
	//
	//		public ArrayList<RankAnnotation> getDataAnnotations(){
	//			return _data_anns;
	//		}
	//
	//		public ArrayList<Set<Nodes>> getData(){
	//			return _data;
	//		}
	//
	//		public RankAnnotation degree(int index){
	//			RankAnnotation ra = _rule_ann.clone();
	//			return ra.meet(_data_anns.get(index));
	//		}
	//
	//		public TreeSet<RankAnnotation> sortedDegrees(){
	//			if(degreeSet==null){
	//				degreeSet = new TreeSet<RankAnnotation>();
	//				for(int i=0; i<_data_anns.size(); i++){
	//					degreeSet.add(degree(i));
	//				}
	//			}
	//			return degreeSet;
	//		}
	//
	//		public String toString(){
	//			StringBuffer buf = new StringBuffer();
	//			buf.append(InconsistencyException.RULE_ID_PREFIX+_rule_id+"\n");
	//			if(_rule_ann!=null)
	//				buf.append(InconsistencyException.RULE_ANNOTATION_PREFIX+_rule_ann+"\n");
	//
	//			int i = 0;
	//			Iterator<RankAnnotation> annIter = null;
	//			if(_data_anns!=null && !_data_anns.isEmpty()){
	//				annIter = _data_anns.iterator();
	//			}
	//
	//			for(Set<Nodes> na :_data){
	//				buf.append(InconsistencyException.INCON_NUMBER_PREFIX+i+"\n");
	//				if(annIter!=null){
	//					buf.append(InconsistencyException.INCON_ANNOTATION_PREFIX+Nodes.toN3(annIter.next().toNodeArray())+"\n");
	//				}
	//				buf.append(InconsistencyException.DATA_HEADER+"\n");
	//				for(Nodes n:na){
	//					buf.append(n.toString()+" ");
	//					buf.append("\n");
	//				}
	//			}
	//			return buf.toString();
	//		}
	//
	//		//	public int compareTo(RankedInconsistencyInfo o) {
	//		//		if(o==this)
	//		//			return 0;
	//		//		if(o._data.size()<=1 && _data.size()>1){
	//		//			return -1;
	//		//		} else if(o._data.size()>1 && o._data.size()<=1){
	//		//			return 1;
	//		//		} else{
	//		//			TreeSet<RankAnnotation> degreeSet1 = new TreeSet<RankAnnotation>();
	//		//			TreeSet<RankAnnotation> degreeSet2 = new TreeSet<RankAnnotation>();
	//		//		}
	//		//	}
	//	}

	public static boolean intersects(Statement s, Set<Node> b){
		return b.contains(s.subject) || b.contains(s.predicate) || b.contains(s.object);
	}

	public static boolean intersects(Node[] s, Set<Node> b){
		for(Node n:s){
			if(b.contains(n))
				return true;
		}
		return false;
	}

	public static Set<Node> getDataNodes(Statement data){
		HashSet<Node> dns = new HashSet<Node>();
		dns.add(data.subject);
		if(!data.predicate.equals(RDF.TYPE)){
			dns.add(data.object);
		}
		return dns;
	}
}

