package org.semanticweb.swse.ann.incons.master;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.rules.AnnotatedRule;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyHandler;
import org.semanticweb.saorr.engine.ih.InconsistencyLogger;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.engine.input.NxaInput;
import org.semanticweb.saorr.rules.GraphPatternRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.saorr.rules.LinkedRuleIndex.LinkedRule;
import org.semanticweb.saorr.rules.SortedRuleSet.SortedLinkedRuleSet;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.incon.RMIAnnInconInterface;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterAnnIncon implements Master<MasterAnnInconArgs>{
	private final static Logger _log = Logger.getLogger(MasterAnnIncon.class.getSimpleName());
	private final static String _var_prefix = "var";

	public MasterAnnIncon(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterAnnInconArgs mia) throws Exception{
		RMIClient<RMIAnnInconInterface> rmic = new RMIClient<RMIAnnInconInterface>(servers, stubName);
		RMIUtils.setLogFile(mia.getMasterLog());

		RMIUtils.mkdirsForFile(mia.getInconsistencyOut());
		RMIUtils.mkdirsForFile(mia.getNewTboxOut());

		_log.log(Level.INFO, "Setting up remote inconsistency job with following args:");
		_log.log(Level.INFO, mia.toString());

		Collection<RMIAnnInconInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote inconsistency detection...");
		Iterator<RMIAnnInconInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteAnnInconInitThread(stubIter.next(), i, servers, mia.getSlaveArgs(i));
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


		_log.log(Level.INFO, "Loading local T-Box...");
		long time = System.currentTimeMillis();
		ResetableIterator<Node[]> tbox = null;
		FileInput fi = null; 
		if(mia.getGzTboxIn())
			fi = new NxaGzInput(new File(mia.getTboxIn()),4);
		else fi = new NxaInput(new File(mia.getTboxIn()),4);
		tbox = new ResetableFlyweightNodeIterator(1000, fi);

		Rules rs = new Rules(mia.getRules());
		rs.setAuthoritative();

		LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules = buildTbox(tbox, rs);

		SortedLinkedRuleSet<AnnotationRule<RankAnnotation>> allLinkedRules = tmplRules.getAllLinkedRules();

		for(LinkedRule<AnnotationRule<RankAnnotation>> tmplRule : allLinkedRules){
			_log.info(tmplRule.getRule()+" (links "+tmplRule.linksCount()+")");
		}

		fi.close();
		_log.log(Level.INFO, "...loaded tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");


		_log.log(Level.INFO, "Extracting and preparing rule patterns...");
		Map<Statement,SortedRuleSet<AnnotationRule<RankAnnotation>>> p2rs = new HashMap<Statement,SortedRuleSet<AnnotationRule<RankAnnotation>>>();
		HashSet<Statement> allPs = new HashSet<Statement>();

		SortedRuleSet<AnnotationRule<RankAnnotation>> joinSRs = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
		SortedRuleSet<AnnotationRule<RankAnnotation>> notJoinSRs = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
		SortedRuleSet<AnnotationRule<RankAnnotation>> unsupported = new SortedRuleSet<AnnotationRule<RankAnnotation>>();

		for(LinkedRule<AnnotationRule<RankAnnotation>> tmplRule : allLinkedRules){
			AnnotationRule<RankAnnotation> ar = tmplRule.getRule();

			Rule r = ar.getPlainRule();

			if(r.isTbox() || (r.getTboxAntecedent()!=null && r.getTboxAntecedent().length>0)){
				//				gpr.isTbox();
				throw new UnsupportedOperationException("Rule "+r+" is not T-ground! The horror... the horror...");
			} else if(!r.isAbox() || r.getAboxAntecedent()==null || r.getAboxAntecedent().length<2){
				joinSRs.add(ar);
			} else if(r instanceof GraphPatternRule){
				GraphPatternRule gpr = (GraphPatternRule)r;
				if(gpr.isTbox() || (gpr.getTboxAntecedent()!=null && gpr.getTboxAntecedent().length>0)){
					//					gpr.isTbox();
					throw new UnsupportedOperationException("Rule "+gpr+" is not T-ground! The horror... the horror...");
				}
				else if(!gpr.isAbox() || gpr.getAboxAntecedent()==null || gpr.getAboxAntecedent().length<2){
					joinSRs.add(ar);
				} else{
					Variable oldv = null, v = null;
					boolean sJoin = true;
					for(Statement s:r.getAboxAntecedent()){
						Node sub = s.subject;
						if(sub instanceof Variable){
							v = (Variable)sub;
						} else{
							sJoin = false;
							break;
						}

						if(oldv==null)
							oldv = v;
						else if(!oldv.equals(v)){
							sJoin = false;
							break;
						}
					}

					if(sJoin){
						joinSRs.add(ar);
					} else{
						notJoinSRs.add(ar);
						for(Statement p: r.getAboxAntecedent()){
							Statement np = normalisePattern(p);
							allPs.add(np);

							SortedRuleSet<AnnotationRule<RankAnnotation>> rset = p2rs.get(np);
							if(rset==null){
								rset = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
								p2rs.put(np, rset);
							}
							rset.add(ar);
						}
					}
				}
			} else{
				_log.severe("Cannot support custom A-Box join rule "+ar);
				unsupported.add(ar);
			}
		}
		_log.log(Level.INFO, "...done...");


		RemoteAnnInconCardinalitiesThread[] cts = new RemoteAnnInconCardinalitiesThread[stubs.size()];

		_log.log(Level.INFO, "Starting remote cardinality detection for "+allPs.size()+" patterns.");
		stubIter = stubs.iterator();
		for(int i=0; i<cts.length; i++){
			cts[i] = new RemoteAnnInconCardinalitiesThread(stubIter.next(), i, allPs);
			cts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<cts.length; i++){
			cts[i].join();
			if(!cts[i].successful()){
				throw cts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" cardinalities returned...");
		}
		_log.log(Level.INFO, "...remote cardinalities extracted.");
		idletime = RMIThreads.idleTime(cts);
		_log.info("Total idle time for co-ordination on cardinalities "+idletime+"...");
		_log.info("Average idle time for co-ordination on cardinalities "+(double)idletime/(double)(cts.length)+"...");

		_log.log(Level.INFO, "Selecting low cardinality patterns...");
		Count<Statement> agg = new Count<Statement>();

		for(RemoteAnnInconCardinalitiesThread ct:cts){
			agg.addAll(ct.getResult());
		}

		int budget = mia.getBudget();
		int size = 0;

		HashSet<Statement> hsp = new HashSet<Statement>();
		HashSet<Statement> lsp = new HashSet<Statement>();
		for(Map.Entry<Statement, Integer> e:agg.getOccurrenceOrderedEntries(true)){
			_log.info("Pattern "+e.getKey()+" size "+e.getValue()+" filled "+size+" budget "+budget);
			if((size+e.getValue())<budget){
				size+=e.getValue();
				hsp.add(e.getKey());
			} else{
				lsp.add(e.getKey());
			}
		}

		SortedRuleSet<AnnotationRule<RankAnnotation>> tbound = new SortedRuleSet<AnnotationRule<RankAnnotation>>();

		tbound.addAll(joinSRs);

		HashSet<Statement> newhsps = new HashSet<Statement>();

		for(AnnotationRule<RankAnnotation> old: notJoinSRs){
			Rule r = old.getPlainRule();

			if(r instanceof GraphPatternRule){
				GraphPatternRule gpr = (GraphPatternRule)r;
				ArrayList<Statement> moveToTbox = new ArrayList<Statement>(); 
				ArrayList<Statement> keepAbox = new ArrayList<Statement>();

				boolean empty = false;
				for(Statement s:gpr.getAboxAntecedent()){
					Statement np = normalisePattern(s);
					if(hsp.contains(np)){
						moveToTbox.add(s);
					} else if(lsp.contains(np)){
						keepAbox.add(s);
					} else{
						_log.info("Skipping: Zero instances for pattern "+s+" in rule "+r);
						empty = true;
						break;
					}
				}

				if(empty){
					continue;
				}

				Statement[] aboxNew = new Statement[keepAbox.size()];
				keepAbox.toArray(aboxNew);
				Statement[] tboxNew = new Statement[moveToTbox.size()];
				moveToTbox.toArray(tboxNew);

				GraphPatternRule gprNew = new GraphPatternRule((gpr.getID()+"_t"), aboxNew, tboxNew, gpr.getFilter(), gpr.getConsequent());
				AnnotationRule<RankAnnotation> annrnew = null;
				if(old instanceof AnnotatedRule<?>){
					AnnotatedRule<?> ar = (AnnotatedRule<?>)old;
					if(RankAnnotation.class.isInstance(ar.getAnnotation()))
						annrnew = RankAnnotationFactory._wrapRule(gprNew, RankAnnotation.class.cast(ar.getAnnotation()));
					else{
						_log.severe("Cannot extract rank annotation from "+old);
						unsupported.add(old);
					}
				} else{
					_log.severe("Cannot extract rank annotation from "+old);
					unsupported.add(old);
				}

				if(annrnew.getAboxAntecedent()!=null && annrnew.getAboxAntecedent().length>1){
					_log.severe("Cannot support rule with two low-selectivity patterns: "+annrnew);
					unsupported.add(annrnew);
				} else{
					tbound.add(annrnew);
					newhsps.addAll(moveToTbox);
				}
			} else{
				_log.severe("Cannot support custom A-Box join rule "+old);
				unsupported.add(old);
			}
		}

		_log.log(Level.INFO, "Extracting new tbox...");

		stubIter = stubs.iterator();
		RemoteAnnInconNewTboxThread[] rrrts = new RemoteAnnInconNewTboxThread[stubs.size()];

		for(int i=0; i<rrrts.length; i++){
			rrrts[i] = new RemoteAnnInconNewTboxThread(stubIter.next(), i, newhsps);
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
			_log.log(Level.INFO, "...new tbox received from "+i+"...");
		}
		_log.log(Level.INFO, "...new tbox received.");

		idletime = RMIThreads.idleTime(rrrts);
		_log.info("Total idle time for co-ordination on new tbox "+idletime+"...");
		_log.info("Average idle time for co-ordination on new tbox "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating tbox...");
		long time2 = System.currentTimeMillis();
		NxParser[] nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		OutputStream os = new FileOutputStream(mia.getNewTboxOut());
		if(mia.getGzNewTboxOut())
			os = new GZIPOutputStream(os);
		CallbackNxOutputStream cb = new CallbackNxOutputStream(os);

		long count = 0;
		for(NxParser nxp:nxps){
			while(nxp.hasNext()){
				cb.processStatement(nxp.next());
				count++;
			}
		}
		os.close();

		for(InputStream is:iss)
			is.close();

		_log.info("...aggregated "+count+" 'new tbox' statements from remote servers in "+(System.currentTimeMillis()-time2)+"ms");

		fi = null;

		ResetableIterator<Node[]> newtbox = null;
		FileInput nfi = null; 
		if(mia.getGzTboxIn())
			nfi = new NxGzInput(new File(mia.getNewTboxOut()));
		else nfi = new NxInput(new File(mia.getNewTboxOut()));
		newtbox = new ResetableFlyweightNodeIterator(1000, nfi);

		os = new FileOutputStream(mia.getInconsistencyOut());
		if(mia.getGzInconsistencyOut())
			os = new GZIPOutputStream(os);

		InconsistencyLogger il = new InconsistencyLogger(new PrintWriter(os));

		LinkedRuleIndex<AnnotationRule<RankAnnotation>> newTmplRules = buildNewTbox(newtbox, tbound, il);

		os.close();

		for(LinkedRule<AnnotationRule<RankAnnotation>> newTmplRule : newTmplRules.getAllLinkedRules()){
			System.err.println(newTmplRule.getRule()+" (links "+newTmplRule.linksCount()+")");
		}

		nfi.close();
		_log.log(Level.INFO, "...aggregated new tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");

		if(!newTmplRules.getAllLinkedRules().isEmpty()){

			_log.log(Level.INFO, "Running remote reasoning...");
			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteAnnInconThread(stubIter.next(), i, newTmplRules);
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
			_log.log(Level.INFO, "...remote inconsistency reasoning done.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on reasoning "+idletime+"...");
			_log.info("Average idle time for co-ordination on reasoning "+(double)idletime/(double)(ibts.length)+"...");

			rmic.clear();
		} else{
			_log.info("No need to run remote inconsistency detection... handled all locally in memory!");
		}

		if(!unsupported.isEmpty()){
			for(Rule r:unsupported){
				_log.severe("Not run "+r);
			}
		}

		_log.log(Level.INFO, "...distributed inconsistency reasoning finished.");
	}

	public static LinkedRuleIndex<AnnotationRule<RankAnnotation>> buildNewTbox(ResetableIterator<Node[]> tboxin, SortedRuleSet<AnnotationRule<RankAnnotation>> tbound, InconsistencyHandler ih) throws Exception{
		SortedRuleSet<Rule> rules = new SortedRuleSet<Rule>(); 
		rules.addAll(tbound);

		ReasonerSettings rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(true);
		rs.setFragment(rules);
		rs.setMergeRules(false);
		rs.setPrintContexts(false);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);

		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(null, tboxin, RankAnnotationFactory.SINGLETON, new CallbackDummy());
		re.setInconsistencyHandler(ih);

		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rs, re);
		r.reason();

		return re.getAboxRuleIndex();
	}

	public static LinkedRuleIndex<AnnotationRule<RankAnnotation>> buildTbox(ResetableIterator<Node[]> tboxin, Rules rules) throws Exception{
		SortedRuleSet<Rule> all = new SortedRuleSet<Rule>();
		all.addAll(Rules.toSet(rules.getRulesArray()));

		ReasonerSettings rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(true);
		rs.setFragment(all);
		rs.setMergeRules(false);
		rs.setPrintContexts(false);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);

		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(null, tboxin, RankAnnotationFactory.SINGLETON, new CallbackDummy());

		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rs, re);
		r.reason();

		return re.getAboxRuleIndex();
	}

	private static Statement normalisePattern(Statement pattern){
		int v = 0;

		Node newSub;
		if(pattern.subject instanceof Variable){
			newSub = new Variable(_var_prefix+v);
			v++;
		} else{
			newSub = pattern.subject;
		}

		Node newPred;
		if(pattern.predicate instanceof Variable){
			if(pattern.predicate.equals(pattern.subject)){
				newPred = newSub;
			} else{
				newPred = new Variable(_var_prefix+v);
				v++;
			}
		} else{
			newPred = pattern.predicate;
		}

		Node newObj;
		if(pattern.object instanceof Variable){
			if(pattern.object.equals(pattern.subject)){
				newObj = newSub;
			} else if(pattern.object.equals(pattern.predicate)){
				newObj = newPred;
			} else {
				newObj = new Variable(_var_prefix+v);
				v++;
			}
		} else{
			newObj = pattern.object;
		}

		return new Statement(newSub, newPred, newObj);
	}
}

