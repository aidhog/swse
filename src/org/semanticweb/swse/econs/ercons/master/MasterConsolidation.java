package org.semanticweb.swse.econs.ercons.master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.fragments.owl2rl.OWL2RL_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.ADHOC_T_SPLIT;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.saorr.rules.LinkedRuleIndex.LinkedRule;
import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;
import org.semanticweb.swse.econs.ercons.master.utils.CardinalityReasoner;
import org.semanticweb.swse.econs.ercons.master.utils.FunctionalReasoner;
import org.semanticweb.swse.econs.ercons.master.utils.InverseFunctionalReasoner;
import org.semanticweb.swse.econs.ercons.master.utils.SameAsReasoner;
import org.semanticweb.swse.file.master.LocalWriteRemoteStreamThread;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterConsolidation implements Master<MasterConsolidationArgs>{
	private final static Logger _log = Logger.getLogger(MasterConsolidation.class.getSimpleName());

	public static final String IFPS_FPS_TEMP = "ifps_fps.nq.gz";

	//	public static final String SAMEAS_TEMP = "sameas.0.nq.gz";
	//	public static final String FPS_TEMP = "fps.0.nq.gz";
	//	public static final String IFPS_TEMP = "ifps.0.nq.gz";

	public static final String SAMEAS_TEMP = "sameas";
	public static final String FPS_TEMP = "fps";
	public static final String IFPS_TEMP = "ifps";
	public static final String CARD_TEMP = "card";


	public MasterConsolidation(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterConsolidationArgs mca) throws Exception{
		RMIClient<RMIConsolidationInterface> rmic = new RMIClient<RMIConsolidationInterface>(servers, stubName);

		RMIUtils.setLogFile(mca.getMasterLog());
		RMIUtils.mkdirs(mca.getTmpDir());


		Collection<RMIConsolidationInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote consolidation...");
		Iterator<RMIConsolidationInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidationInitThread(stubIter.next(), i, servers, mca.getSlaveArgs().instantiate(i), stubName);
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

		_log.log(Level.INFO, "Getting tbox...");

		stubIter = stubs.iterator();
		RemoteConsolidationExtractTboxThread[] rcss = new RemoteConsolidationExtractTboxThread[stubs.size()];

		for(int i=0; i<rcss.length; i++){
			rcss[i] = new RemoteConsolidationExtractTboxThread(stubIter.next(), i);
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
			_log.log(Level.INFO, "...ifps and fps received from "+i+"...");
		}
		_log.log(Level.INFO, "...ifps and fps received.");

		idletime = RMIThreads.idleTime(rcss);
		_log.info("Total idle time for co-ordination on gettings ifps/fps "+idletime+"...");
		_log.info("Average idle time for co-ordination on gettings ifps/fps "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating tbox...");
		long time = System.currentTimeMillis();
		NxParser[] nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		String tbxout = mca.getTboxOut();
		OutputStream os = new GZIPOutputStream(new FileOutputStream(tbxout));
		Callback cb = new CallbackNxOutputStream(os);
		mergeIterators(cb, nxps);

		os.close();

		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}

		long time2 = System.currentTimeMillis();
		_log.log(Level.INFO, "...aggregated tbox pairs in "+(time2-time)+" ms");

		_log.info("Identifying and loading consolidatable patterns...");
		Rules rules = MasterConsolidationArgs.CONSOLIDATION_ONLY_RULES;
		boolean auth = true;

		Set<Statement> patterns = new HashSet<Statement>();
		patterns.add(new Statement(new Variable("x"), OWL.SAMEAS, new Variable("y")));

		//		Rules atRules = new Rules(rules.getTboxAboxRules());

		FileInput tboxin = null;
		if(auth){
			tboxin = new NxaGzInput(new File(tbxout), 4);
		} else{
			tboxin = new NxGzInput(new File(tbxout));
		}

		//		LinkedRuleIndex<Rule> ruls = MasterReasoner.buildTbox(tboxin, atRules, new CallbackDummy(), auth);
		//		ruls.freeResources();

		if(auth){
			rules.setAuthoritative();
		}

		SortedRuleSet<Rule> tbox = Rules.toSet(rules.getTboxRules());

		ReasonerSettings rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(auth);
		rs.setFragment(tbox);
		rs.setMergeRules(false);
		rs.setPrintContexts(true);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);

		ReasonerEnvironment re = new ReasonerEnvironment(null, tboxin, new CallbackDummy());

		Reasoner r = new Reasoner(rs, re);
		r.reason();

		LinkedRuleIndex<Rule> ruls = re.getAboxRuleIndex();
		ruls.freeResources();

		HashSet<Node> ifps = new HashSet<Node>();
		HashSet<Node> fps = new HashSet<Node>();
		HashMap<Node,HashSet<Node>> cards = new HashMap<Node,HashSet<Node>>();
		for(LinkedRule<Rule> lr:ruls.getAllLinkedRules()){
			Rule rul = lr.getRule();
			for(Statement s:rul.getAboxAntecedent()){
				patterns.add(s);
			}
			if(rul.getID().contains(OWL2RL_T_SPLIT.PRP_IFP.getID())){
				ifps.add(rul.getAboxAntecedent()[0].predicate);
			} else if (rul.getID().contains(OWL2RL_T_SPLIT.PRP_FP.getID())){
				fps.add(rul.getAboxAntecedent()[0].predicate);
			} else if (rul.getID().contains(OWL2RL_T_SPLIT.CLS_MAXC2.getID()) || rul.getID().contains(ADHOC_T_SPLIT.CLS_C2.getID())){
				Node c = null, p = null;
				for(Statement s:rul.getAboxAntecedent()){
					if(s.object instanceof Variable){
						p = s.predicate;
					} else{
						c = s.object;
					}
				}
				HashSet<Node> preds = cards.get(c);
				if(preds==null){
					preds = new HashSet<Node>();
					cards.put(c, preds);
				}
				preds.add(p);
			}
		}

		_log.info("IFPS: "+ifps);
		_log.info("FPS: "+fps);
		_log.info("Cards: "+cards);

		_log.info("...identified and loaded consolidatable patterns took "+(System.currentTimeMillis()-time2)+" ms.");

		String remotetbox = null;
		Rule[] rulez = null;

		if(mca.getReasonExtract()){
			_log.log(Level.INFO, "Opening remote outputstreams ...");
			remotetbox = mca.getSlaveArgs().getOutDir() + "/" + MasterConsolidationArgs.DEFAULT_TBOX_FILENAME_GZ;
			RemoteOutputStreamThread[] rosts = new RemoteOutputStreamThread[stubs.size()];
			stubIter = stubs.iterator();
			for(int i=0; i<rosts.length; i++){
				rosts[i] = new RemoteOutputStreamThread(stubIter.next(), i, RMIUtils.getLocalName(remotetbox,i));
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
				ibts[j] = new LocalWriteRemoteStreamThread(new FileInputStream(tbxout), oss[j], j);
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

			rulez = MasterConsolidationArgs.GENERAL_REASONING_RULES.getRulesArray();
		}


		_log.log(Level.INFO, "Getting remote consolidatable statements...");

		stubIter = stubs.iterator();
		RemoteConsolidationTriplesThread[] rcts = new RemoteConsolidationTriplesThread[stubs.size()];

		for(int i=0; i<rcss.length; i++){
			String rt = null;
			if(remotetbox!=null){
				rt = RMIUtils.getLocalName(remotetbox, i);
			}

			rcts[i] = new RemoteConsolidationTriplesThread(stubIter.next(), i, patterns, rt, rulez);
			rcts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		iss = new InputStream[rcts.length];
		for(int i=0; i<rcts.length; i++){
			rcts[i].join();
			if(!rcts[i].successful()){
				throw rcts[i].getException();
			}
			iss[i] = RemoteInputStreamClient.wrap(rcts[i].getResult());
			_log.log(Level.INFO, "...consolidation triples received from "+i+"...");
		}
		_log.log(Level.INFO, "...consolidation triples received.");

		idletime = RMIThreads.idleTime(rcts);
		_log.info("Total idle time for co-ordination on gettings consolidation triples "+idletime+"...");
		_log.info("Average idle time for co-ordination on gettings consolidation triples "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Locally aggregating consolidation triples...");
		long time1b= System.currentTimeMillis();
		nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		//		String ifps_file = RMIUtils.getLocalName(outdir)+"/"+IFPS_TEMP;
		//		String fps_file = RMIUtils.getLocalName(outdir)+"/"+FPS_TEMP;
		//		String sameas_file = RMIUtils.getLocalName(outdir)+"/"+SAMEAS_TEMP;

		//		OutputStream ifps_os = new GZIPOutputStream(new FileOutputStream(ifps_file));
		//		OutputStream fps_os = new GZIPOutputStream(new FileOutputStream(fps_file));
		//		OutputStream sameas_os = new GZIPOutputStream(new FileOutputStream(sameas_file));

		//		Callback ifps = new CallbackNxOutputStream(ifps_os);
		//		Callback fps_c = new CallbackNxOutputStream(fps_os);
		//		Callback sameas_c = new CallbackNxOutputStream(sameas_os);

		InverseFunctionalReasoner ifr = new InverseFunctionalReasoner(mca.getTmpDir()+"/"+IFPS_TEMP+ "/");
		FunctionalReasoner fr = new FunctionalReasoner(mca.getTmpDir()+"/"+FPS_TEMP+ "/", false);
		CardinalityReasoner cr = new CardinalityReasoner(mca.getTmpDir()+"/"+CARD_TEMP+ "/", cards, false);
		SameAsReasoner sar = new SameAsReasoner(mca.getTmpDir()+"/"+SAMEAS_TEMP+ "/");

		CallbackConsolTriples ifsc_c = new CallbackConsolTriples(sar, ifps, ifr, fps, fr, cards, cr);

		mergeIterators(ifsc_c, nxps);

		_log.info("...collected "+ifsc_c.sameAsTripleCount()+" sameAs triples.");
		_log.info("...collected "+ifsc_c.ifpTripleCount()+" IFP triples.");
		_log.info("...collected "+ifsc_c.fpTripleCount()+" FP triples.");
		_log.info("...collected "+ifsc_c.cardTripleCount()+" Card triples.");

		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}

		long time2b = System.currentTimeMillis();
		_log.log(Level.INFO, "...aggregated consolidation triples in "+(time2b-time1b)+" ms");


		if(!mca.getSameasOnly()){
			_log.log(Level.INFO, "Calling remote sort...");

			stubIter = stubs.iterator();

			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteConsolidationSortThread(stubIter.next(), i);
				ibts[i].start();
			}
		}

		long time3b = System.currentTimeMillis();
		_log.log(Level.INFO, "Locally computing sameAs while we wait...");
		String closedSA = mca.getSameAsOut();
		computeSameas(sar, ifr, fr, cr, closedSA);
		sar.close();
		fr.close();
		ifr.close();
		_log.info("Locally computing sameAs files took "+(System.currentTimeMillis()-time3b)+" ms.");
		
		if(mca.getSameasOnly()){
			_log.log(Level.INFO, "...distributed same-as closure finished.");
			return;
		}

		_log.log(Level.INFO, "...awaiting thread return for remote sort...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "...remote threads sorted.");

			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sorting "+idletime+"...");
			_log.info("Average idle time for co-ordination on sorting "+(double)idletime/(double)(ibts.length)+"...");
		}

		_log.log(Level.INFO, "Opening remote outputstreams for sameas...");
		String remotesa = mca.getSlaveArgs().getOutDir() + "/" + MasterConsolidationArgs.DEFAULT_SAMEAS_FILENAME;
		RemoteOutputStreamThread[] rosts = new RemoteOutputStreamThread[stubs.size()];
		stubIter = stubs.iterator();
		for(int i=0; i<rosts.length; i++){
			rosts[i] = new RemoteOutputStreamThread(stubIter.next(), i, RMIUtils.getLocalName(remotesa,i));
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
			ibts[j] = new LocalWriteRemoteStreamThread(new FileInputStream(closedSA), oss[j], j);
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

		_log.log(Level.INFO, "Running remote consolidation...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidationThread(stubIter.next(), i, RMIUtils.getLocalName(remotesa, i));
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting remote consolidation thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" consolidated...");
		}
		_log.log(Level.INFO, "...remote consolidation consolidated.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on consolidation "+idletime+"...");
		_log.info("Average idle time for co-ordination on consolidation "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "...distributed consolidation finished.");
	}

	private void computeSameas(SameAsReasoner sar, InverseFunctionalReasoner ifr, FunctionalReasoner fr, CardinalityReasoner cr, String finalsa) throws FileNotFoundException, IOException, ParseException {
		int saiter = 0;

		//		_odrm.printStats();


		boolean sarEmpty = !sar.hasChanged();
		boolean ifpEmpty = ifr==null || !ifr.hasChanged();
		boolean fpEmpty = fr==null || !fr.hasChanged();
		boolean cEmpty = cr==null || !cr.hasChanged();

		boolean sarChangedIFP = !ifpEmpty;
		boolean sarChangedFP = !fpEmpty;
		boolean sarChangedC = !cEmpty;

		boolean done = ifpEmpty && fpEmpty && cEmpty;

		int c = 0, f = 0, i = 0;

		Iterator<Node[]> sameas;
		while(!done){
			long b4 = 0, after = 0, count = 0;
			if(!ifpEmpty && (sarChangedIFP || sarChangedFP || sarChangedC)){
				_log.info("Performing inverse functional property reasoning...");
				b4 = System.currentTimeMillis();
				if(saiter==0 || sar.getSameAsIndex()==null){
					sameas = ifr.performReasoning(null);
				} else{
					String s = sar.getSameAsIndex();
					InputStream sais = new GZIPInputStream(new FileInputStream(s));
					NxParser nxp = new NxParser(sais);
					sameas = ifr.performReasoning(nxp);
					sais.close();
				}

				_log.info("...done!");
				after = System.currentTimeMillis();

				_log.info("Performing inverse functional property reasoning took "+(after-b4));
				count=0;

				sar.resetHasChangedFlag();
				while(sameas.hasNext()){
					count++;
					Node[] sa = sameas.next();
					sar.addStatement(sa);
					i++;
				}
				sarChangedIFP = sar.hasChanged() || (saiter==0 && !sarEmpty);
				_log.info("Reasoning on "+count+" inverse functional property results took "+(System.currentTimeMillis()-after));
			}

			if(!fpEmpty && (sarChangedIFP || sarChangedFP || sarChangedC)){
				_log.info("Performing functional property reasoning...");
				b4 = System.currentTimeMillis();
				if(saiter==0 || sar==null || sar.getSameAsIndex()==null){
					sameas = fr.performReasoning(null);
				} else{
					String s = sar.getSameAsIndex();
					InputStream sais = new GZIPInputStream(new FileInputStream(s));
					NxParser nxp = new NxParser(sais);
					sameas = fr.performReasoning(nxp);
					sais.close();
				}

				_log.info("...done!");
				after = System.currentTimeMillis();

				_log.info("Performing functional property reasoning took "+(after-b4));
				count=0;

				sar.resetHasChangedFlag();
				while(sameas.hasNext()){
					count++;
					sar.addStatement(sameas.next());
					f++;
				}
				_log.info("Reasoning on "+count+" functional property results took "+(System.currentTimeMillis()-after));
				sarChangedFP = sar.hasChanged() || (saiter==0 && !sarEmpty);
			}

			if(!cEmpty && (sarChangedIFP || sarChangedFP || sarChangedC)){
				_log.info("Performing cardinality reasoning...");
				b4 = System.currentTimeMillis();
				if(saiter==0 || sar==null || sar.getSameAsIndex()==null){
					sameas = cr.performReasoning(null);
				} else{
					String s = sar.getSameAsIndex();
					InputStream sais = new GZIPInputStream(new FileInputStream(s));
					NxParser nxp = new NxParser(sais);
					sameas = cr.performReasoning(nxp);
					sais.close();
				}

				_log.info("...done!");
				after = System.currentTimeMillis();

				_log.info("Performing cardinality reasoning took "+(after-b4));
				count=0;

				sar.resetHasChangedFlag();
				while(sameas.hasNext()){
					count++;
					sar.addStatement(sameas.next());
					c++;
				}
				_log.info("Reasoning on "+count+" cardinality results took "+(System.currentTimeMillis()-after));
				sarChangedC = sar.hasChanged() || (saiter==0 && !sarEmpty);
			}

			//if sai changes once, all join indices need to be re-done
			done = !sarChangedFP && !sarChangedIFP && !sarChangedC;
			saiter++;
		}

		_log.info("Card "+c+" FP "+f+" IFP "+i);

		InputStream is = new FileInputStream(sar.getSameAsIndex());
		is = new GZIPInputStream(is);
		NxParser nxp = new NxParser(is);

		_log.info("Writing final sameas results from "+sar.getSameAsIndex()+" to "+finalsa);
		OutputStream os = new FileOutputStream(finalsa);
		os = new GZIPOutputStream(os);
		CallbackNxOutputStream cb = new CallbackNxOutputStream(os);

		Node[] line;
		int out = 0;
		while(nxp.hasNext()){
			line = nxp.next();
			if(line[0].compareTo(line[2])>0){
				cb.processStatement(line);
				out++;
			}
		}
		_log.info("Printed "+out+" non-symmetric, non-reflexive, non-transitive sameas triples "+out);

		os.close();
		is.close();
	}

	//	private static Set<Statement> identifyConsolidatablePatterns(String tbox, Rules rules, boolean auth) throws Exception {
	//		Set<Statement> patterns = new HashSet<Statement>();
	//		patterns.add(new Statement(new Variable("x"), OWL.SAMEAS, new Variable("y")));
	//		
	//		Rules atRules = new Rules(rules.getTboxAboxRules());
	//		
	//		FileInput tboxin = null;
	//		if(auth){
	//			tboxin = new NxGzInput(new File(tbox));
	//		} else{
	//			tboxin = new NxaGzInput(new File(tbox), 4);
	//		}
	//		
	//		LinkedRuleIndex<Rule> ruls = MasterReasoner.buildTbox(tboxin, atRules, new CallbackDummy(), auth);
	//		ruls.freeResources();
	//		
	//		for(LinkedRule<Rule> lr:ruls.getAllLinkedRules()){
	//			Rule r = lr.getRule();
	//			for(Statement s:r.getAboxAntecedent())
	//				patterns.add(s);
	//		}
	//
	//		return patterns;
	//	}

	private static void mergeIterators(Callback cb, Iterator<Node[]>... in){
		boolean done = false;
		while(!done){
			done = true;
			for(Iterator<Node[]> i:in){
				if(i.hasNext()){
					done = false;
					Node[] next = i.next();
					cb.processStatement(next);
				}
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

	public static class HashSetNode extends HashSet<Node>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 8505886232846983023L;
		;
	}

	public static class CallbackIFPsFPs implements Callback{

		private HashSetNode _ifps;
		private HashSetNode _fps;

		public CallbackIFPsFPs(){
			_ifps = new HashSetNode();
			_fps = new HashSetNode();
		}

		public void endDocument() {
			;
		}

		public void processStatement(Node[] stmt) {
			if(stmt[1].equals(OWL.INVERSEFUNCTIONALPROPERTY)){
				_ifps.add(stmt[0]);
				_log.info("Found IFP through reasoning "+Nodes.toN3(stmt));
			} else if(stmt[1].equals(OWL.FUNCTIONALPROPERTY)){
				_fps.add(stmt[0]);
				_log.info("Found FP through reasoning "+Nodes.toN3(stmt));
			}
		}

		public HashSetNode getIFPs(){
			return _ifps;
		}

		public HashSetNode getFPs(){
			return _fps;
		}

		public void startDocument() {
			;
		}

	}

	public static class CallbackConsolTriples implements Callback{

		private Set<Node> _ifps;
		private Set<Node> _fps;
		private Set<Node> _cardsPs;
		private Set<Node> _cardsCs;

		private int _cs = 0;
		private int _ci = 0;
		private int _cf = 0;
		private int _cc = 0;

		private Callback _csameas;
		private Callback _cifps;
		private Callback _cfps;
		private Callback _ccard;

		public CallbackConsolTriples(Callback csameas, Set<Node> ifps, Callback cifps, Set<Node> fps, Callback cfps, Map<Node,? extends Set<Node>> cards, Callback ccard){
			_csameas = csameas;
			_cifps = cifps;
			_cfps = cfps;
			_ccard = ccard;

			_ifps = ifps;
			_fps = fps;

			_cardsPs = new HashSet<Node>();
			_cardsCs = new HashSet<Node>();
			if(cards!=null) for (Map.Entry<Node, ? extends Set<Node>> e:cards.entrySet()){
				_cardsPs.addAll(e.getValue());
				_cardsCs.add(e.getKey());
			}
		}

		public void endDocument() {
			_csameas.endDocument();
			_cifps.endDocument();
			_cfps.endDocument();
		}

		public int ifpTripleCount(){
			return _ci;
		}

		public int fpTripleCount(){
			return _cf;
		}

		public int sameAsTripleCount(){
			return _cs;
		}

		public int cardTripleCount(){
			return _cc;
		}

		public void processStatement(Node[] stmt) {
			if(stmt[1].equals(OWL.SAMEAS)){
				_csameas.processStatement(stmt);
				_cs++;
			} else if(_ifps.contains(stmt[1])){
				_cifps.processStatement(stmt);
				_ci++;
			} else if(_fps.contains(stmt[1])){
				_cfps.processStatement(stmt);
				_cf++;
			} else if(_cardsPs.contains(stmt[1])){
				_ccard.processStatement(stmt);
				_cc++;
			} else if(stmt[1].equals(RDF.TYPE) && _cardsCs.contains(stmt[2])){
				_ccard.processStatement(stmt);
				_cc++;
			}
		}

		public void startDocument() {
			_csameas.startDocument();
			_cifps.startDocument();
			_cfps.startDocument();
		}

	}
}
