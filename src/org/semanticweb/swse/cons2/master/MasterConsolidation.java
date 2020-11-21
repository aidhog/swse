package org.semanticweb.swse.cons2.master;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.auth.AuthorityInspector;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons2.RMIConsolidationConstants;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;
import org.semanticweb.swse.cons2.master.utils.FunctionalReasoner;
import org.semanticweb.swse.cons2.master.utils.InverseFunctionalReasoner;
import org.semanticweb.swse.cons2.master.utils.SameAsReasoner;
import org.semanticweb.swse.cons2.utils.CONS_RULES;
import org.semanticweb.swse.cons2.utils.PreprocessAuthority;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
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
public class MasterConsolidation {
	private final static Logger _log = Logger.getLogger(MasterConsolidation.class.getSimpleName());

	public static final String IFPS_FPS_TEMP = "ifps_fps.nq.gz";

//	public static final String SAMEAS_TEMP = "sameas.0.nq.gz";
//	public static final String FPS_TEMP = "fps.0.nq.gz";
//	public static final String IFPS_TEMP = "ifps.0.nq.gz";

	public static final String SAMEAS_TEMP = "sameas";
	public static final String FPS_TEMP = "fps";
	public static final String IFPS_TEMP = "ifps";

	private RMIRegistries _servers;
	private RMIClient<RMIConsolidationInterface> _rmic;

	public MasterConsolidation(RMIRegistries servers) throws RemoteException, NotBoundException{
		_servers = servers;
		_rmic = new RMIClient<RMIConsolidationInterface>(servers, RMIConsolidationConstants.DEFAULT_STUB_NAME);
	}

	public void start(String infile, boolean gzin, boolean reason, AuthorityInspector ai, String outdir, String tmpdir) throws Exception{
		Collection<RMIConsolidationInterface> stubs = _rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		String localtmp = RMIUtils.getLocalName(tmpdir);
		RMIUtils.mkdirs(localtmp);

		String localout = RMIUtils.getLocalName(outdir);
		RMIUtils.mkdirs(localout);

		_log.log(Level.INFO, "Initialising remote consolidation...");
		Iterator<RMIConsolidationInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidationInitThread(stubIter.next(), i, _servers, RMIUtils.getLocalName(infile, i), gzin, RMIUtils.getLocalName(outdir, i), RMIUtils.getLocalName(tmpdir, i));
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

		_log.log(Level.INFO, "Getting ifps and fps...");

		stubIter = stubs.iterator();
		RemoteConsolidationIFPsFPsThread[] rcss = new RemoteConsolidationIFPsFPsThread[stubs.size()];

		for(int i=0; i<rcss.length; i++){
			rcss[i] = new RemoteConsolidationIFPsFPsThread(stubIter.next(), i, reason);
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

		_log.log(Level.INFO, "Locally aggregating ifps/fps...");
		long time = System.currentTimeMillis();
		NxParser[] nxps = new NxParser[iss.length];
		for(int i=0; i<nxps.length; i++){
			nxps[i] = new NxParser(new GZIPInputStream(iss[i]));
		}

		String ifps_fps_f = localout+"/"+IFPS_FPS_TEMP;
		OutputStream os = new GZIPOutputStream(new FileOutputStream(ifps_fps_f));
		Callback cb = new CallbackNxOutputStream(os);
		mergeIterators(cb, nxps);

		os.close();

		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}

		long time2 = System.currentTimeMillis();
		_log.log(Level.INFO, "...aggregated ifps/fps pairs in "+(time2-time)+" ms");

		_log.info("Identifying and loading ifp/fps...");
		HashSet<Node>[] ifps_fps = identifyIFPsandFPs(ifps_fps_f, reason, ai);
		_log.info("...identifying and loading ifps/fps took "+(System.currentTimeMillis()-time2)+" ms.");

		_log.log(Level.INFO, "Getting remote consolidatable statements...");

		stubIter = stubs.iterator();
		RemoteConsolidationTriplesThread[] rcts = new RemoteConsolidationTriplesThread[stubs.size()];

		for(int i=0; i<rcss.length; i++){
			rcts[i] = new RemoteConsolidationTriplesThread(stubIter.next(), i, ifps_fps[0], ifps_fps[1]);
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

		InverseFunctionalReasoner ifr = new InverseFunctionalReasoner(localtmp+"/"+IFPS_TEMP+ "/");
		FunctionalReasoner fr = new FunctionalReasoner(localtmp+"/"+FPS_TEMP+ "/", false);
		SameAsReasoner sar = new SameAsReasoner(localtmp+"/"+SAMEAS_TEMP+ "/");

		CallbackConsolTriples ifs_c = new CallbackConsolTriples(sar, ifps_fps[0], ifr, ifps_fps[1], fr);

		mergeIterators(ifs_c, nxps);



		_log.info("...collected "+ifs_c.sameAsTripleCount()+" sameAs triples.");
		_log.info("...collected "+ifs_c.ifpTripleCount()+" IFP triples.");
		_log.info("...collected "+ifs_c.fpTripleCount()+" FP triples.");

		for(int i=0; i<iss.length; i++){
			iss[i].close();
		}

		long time2b = System.currentTimeMillis();
		_log.log(Level.INFO, "...aggregated consolidation triples in "+(time2b-time1b)+" ms");


		_log.log(Level.INFO, "Calling remote sort...");

		stubIter = stubs.iterator();

		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidationSortThread(stubIter.next(), i);
			ibts[i].start();
		}

		long time3b = System.currentTimeMillis();
		_log.log(Level.INFO, "Locally computing sameAs while we wait...");
		String closedSA = computeSameas(sar, ifr, fr);
		sar.close();
		fr.close();
		ifr.close();
		_log.info("Locally computing sameAs files took "+(System.currentTimeMillis()-time3b)+" ms.");

		_log.log(Level.INFO, "Flooding sameAs while we wait...");
		RemoteConsolidationGatherSameasThread[] rcgsts = new RemoteConsolidationGatherSameasThread[stubs.size()];

		RemoteInputStreamServer istream = null;
		for(int i=0; i<rcgsts.length; i++){
			try {
				istream = new SimpleRemoteInputStream(new BufferedInputStream(
						new FileInputStream(closedSA)));
				RemoteInputStream result = istream.export();
				istream = null;

				_log.info("Flooding "+closedSA+" to remote server "+i+"...");
				RMIConsolidationInterface rmii = _rmic.getStub(i);

				RemoteConsolidationGatherSameasThread rcgst = new RemoteConsolidationGatherSameasThread(rmii, i, result);

				rcgst.start();

				rcgsts[i] = rcgst;
			} catch(IOException e){ 
				_log.log(Level.SEVERE, "Error creating RemoteInputStream on master server\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error creating RemoteInputStream on master server\n"+e);
			} finally {
				// we will only close the stream here if the server fails before
				// returning an exported stream
				if(istream != null) istream.close();
			}
		}

		_log.log(Level.INFO, "...awaiting thread return for remote sameas flood...");
		for(int i=0; i<rcgsts.length; i++){
			rcgsts[i].join();
			if(!rcgsts[i].successful()){
				throw rcgsts[i].getException();
			}
			_log.log(Level.INFO, "...remote sameas flood received on "+i);
		}
		_log.log(Level.INFO, "...remote sameas flood received.");

		idletime = RMIThreads.idleTime(rcgsts);
		_log.info("Total idle time for co-ordination on sorting "+idletime+"...");
		_log.info("Average idle time for co-ordination on sorting "+(double)idletime/(double)(rcgsts.length)+"...");

		_log.log(Level.INFO, "...awaiting thread return for remote sort...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
		}
		_log.log(Level.INFO, "...remote threads sorted.");

		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on sorting "+idletime+"...");
		_log.info("Average idle time for co-ordination on sorting "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Running remote consolidation...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteConsolidateThread(stubIter.next(), i);
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

	private String computeSameas(SameAsReasoner sar, InverseFunctionalReasoner ifr, FunctionalReasoner fr) throws FileNotFoundException, IOException, ParseException {
		boolean done = !sar.hasChanged() && (ifr==null || !ifr.hasChanged()) && (fr==null || !fr.hasChanged());
		int saiter = 0;

//		_odrm.printStats();
		
		boolean sarChangedIFP = true;
		boolean sarChangedFP = true;
		boolean sarEmpty = !sar.hasChanged();
		boolean ifpEmpty = ifr==null || !ifr.hasChanged();
		boolean fpEmpty = fr==null || !fr.hasChanged();
		
		done = ifpEmpty && fpEmpty;
		
		Iterator<Node[]> sameas;
		while(!done){
			long b4 = 0, after = 0, count = 0;
			if(!ifpEmpty && (sarChangedIFP || sarChangedFP)){
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

				System.err.println("...done!");
				after = System.currentTimeMillis();

				System.out.println("Performing inverse functional property reasoning took "+(after-b4));
				count=0;
				
				sar.resetHasChangedFlag();
				while(sameas.hasNext()){
					count++;
					Node[] sa = sameas.next();
					sar.addStatement(sa);
				}
				sarChangedIFP = sar.hasChanged() || (saiter==0 && !sarEmpty);
				System.out.println("Reasoning on "+count+" inverse functional property results took "+(System.currentTimeMillis()-after));

//				_stats.incrementRule(22, count);
//				_odrm.printStats();
			}

			if(!fpEmpty && (sarChangedIFP || sarChangedFP)){
				System.err.println("Performing functional property reasoning...");
				System.out.println("Performing functional property reasoning...");
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

				System.err.println("...done!");
				after = System.currentTimeMillis();

				System.out.println("Performing functional property reasoning took "+(after-b4));
				count=0;

				sar.resetHasChangedFlag();
				while(sameas.hasNext()){
					count++;
					sar.addStatement(sameas.next());
				}
				System.out.println("Reasoning on "+count+" functional property results took "+(System.currentTimeMillis()-after));
				sarChangedFP = sar.hasChanged() || (saiter==0 && !sarEmpty);
				
				done = sarChangedFP || sarChangedIFP;
//				_stats.incrementRule(21, count);
//				_odrm.printStats();
			}

			//if sai changes once, all join indices need to be re-done
			done = !sarChangedFP && !sarChangedIFP;
			saiter++;
		}

		return sar.getSameAsIndex();
	}

	private static HashSetNode[] identifyIFPsandFPs(String ifps_fps, boolean reason,
			AuthorityInspector ai) throws ParseException {

		HashSetNode[] hsn = new HashSetNode[2];

		hsn[0] = new HashSetNode();
		hsn[1] = new HashSetNode();

		NxGzInput nxi = new NxGzInput(new File(ifps_fps));

		_log.info("Extracting IFPs/FPs from data "+ifps_fps+".");
		while(nxi.hasNext()){
			Node[] next = nxi.next();
			if(next[2].equals(OWL.INVERSEFUNCTIONALPROPERTY)){
				if(ai.checkAuthority(next[0], next[3])){
					hsn[0].add(next[0]);
					_log.info("Found asserted IFP "+Nodes.toN3(next));
				} else{
					_log.info("Ignoring non-auth IFP "+Nodes.toN3(next));
				}
			} else if(next[2].equals(OWL.FUNCTIONALPROPERTY)){
				if(ai.checkAuthority(next[0], next[3])){
					hsn[1].add(next[0]);
					_log.info("Found asserted FP "+Nodes.toN3(next));
				} else{
					_log.info("Ignoring non-auth FP "+Nodes.toN3(next));
				}
			}
		}


		if(reason){
			nxi.reset();
			PreprocessAuthority pa = new PreprocessAuthority(nxi, ai);
			ReasonerSettings rs = new ReasonerSettings();
			rs.setAuthorativeReasoning(false);
			rs.setFragment(Fragment.getRules(CONS_RULES.class));
			rs.setPrintContexts(true);
			rs.setSkipAxiomatic(true);
			rs.setSkipTBox(true);

			CallbackIFPsFPs cif = new CallbackIFPsFPs();
			ReasonerEnvironment re = new ReasonerEnvironment(pa, cif);

			Reasoner r = new Reasoner(rs, re);
			try{
				r.reason();
			} catch(Exception ie){
				_log.severe(ie.getMessage());
			}

			hsn[0].addAll(cif.getIFPs());
			hsn[1].addAll(cif.getFPs());
		}

		return hsn;
	}

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

		private int _cs = 0;
		private int _ci = 0;
		private int _cf = 0;

		private Callback _csameas;
		private Callback _cifps;
		private Callback _cfps;

		public CallbackConsolTriples(Callback csameas, Set<Node> ifps, Callback cifps, Set<Node> fps, Callback cfps){
			_csameas = csameas;
			_cifps = cifps;
			_cfps = cfps;

			_ifps = ifps;
			_fps = fps;
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
			}
		}

		public void startDocument() {
			_csameas.startDocument();
			_cifps.startDocument();
			_cfps.startDocument();
		}

	}
}
