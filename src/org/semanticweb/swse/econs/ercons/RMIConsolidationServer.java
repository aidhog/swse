package org.semanticweb.swse.econs.ercons;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.auth.RedirectsAuthorityInspector;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.engine.tbox.TboxExtractor;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.incons.utils.PatternIndex;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator.HandleNode;
import org.semanticweb.swse.saor.master.MasterReasoner;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.XSD;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIConsolidationServer implements RMIConsolidationInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final static Logger _log = Logger.getLogger(RMIConsolidationServer.class.getSimpleName());
	private final static int TICKS = 10000000;
	
	private boolean _sorting = false;
	private boolean _gathering = false;
	
	private String _sorted = null;
	private String _sameas = null;


	public static final Node[] IFP_BLACKLIST = { 
		new Literal("08445a31a78661b5c746feff39a9db6e4e2cc5cf"),
		new Literal("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
		new Literal(""),
		new Literal(" "),
		new Literal("  "),
		new Literal("N/A"),
		new Literal("n/a"),
		new Literal("none"),
		new Literal("ask"),
		new Literal("ask me"),
		new Literal("empty"),
		new Literal("null"),
		new Literal("blank"),
		new Literal("?"),
		new Literal("??"),
		new Literal("???"),
		new Literal("not saying"),
		new Literal("not telling"),
		new Literal("private"),
		new Literal("blah"),
		new Literal("no"),
		new Literal("n"),
		new Literal("N"),
		new Resource(""),
		new Resource(" "),
		new Resource("mailto:"),
		new Resource("http://"),
		new Resource("http://(null)"),
		new Resource("http://$http://null"),
		new Resource("http://+http://null"),
		new Resource("http:////null"),
		new Resource("http://null."),
		new Resource("http://null.com"),
		new Resource("http://null/"),
		new Resource("http://null"),
		new Resource("http://facebook.com"),
		new Resource("http://facebook.com/"),
		new Resource("http://www.facebook.com"),
		new Resource("http://www.facebook.com/"),
		new Resource("http://google.com/"),
		new Resource("http://google.com"),
		new Resource("http://www.google.com/"),
		new Resource("http://www.google.com"),
		new BNode(""),
		new Resource("http://www.vox.com/gone/")
		
	};

	public static final HashSet<Node> IFP_BLACKLIST_HS = new HashSet<Node>();  

	static{
		for(Node n:IFP_BLACKLIST){
			IFP_BLACKLIST_HS.add(n);
		}
	}

	public final static String IFPS_FPS_FILE = "ifps-fps.nq.gz";
	public final static String CONSOLIDATION_TRIPLES = "cons-triples.gz";
	public final static String OUTPUT_FILE = "data.cons2.nq.gz";

	public final static String SORTED_S_FILE = "data.sorted.s.gz";
	public final static String SORTED_O_FILE = "data.sorted.o.gz";

	public final static String SAMEAS_FILE = "sameas.final.nq.gz";

	public final static String CONS_S_FILE = "data.cons.s.nq.gz";
	public final static String CONS_O_FILE = "data.cons.o.nq.gz";

	public final static int[] O_ORDER = new int[]{2,1,0,3,4};

	public final static int S = 0;
	public final static int O = 2;

	private int _serverID = -1;
	private RMIRegistries _servers;

	private Vector<String> _toGatherFn = null;
	RMIClient<RMIConsolidationInterface> _rmic = null;

	private SlaveConsolidationArgs _sca;

	//	private String _infile;
	//	private String _outdir;
	//	private String _tmpdir;
	//	private boolean _ingz;
	//	private String _sorted;
	//	private String _sameas;
	//	
	//	private boolean _sorting = false;
	//	private boolean _gathering = false;

	public RMIConsolidationServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveConsolidationArgs sca, String stubName) throws RemoteException {
		long b4 = System.currentTimeMillis();
		try {
			RMIUtils.setLogFile(sca.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sca.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sca.getSlaveLog()+" on server "+serverId+"\n"+e);
		}

		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sca);

		_sca = sca;

		_toGatherFn = new Vector<String>();

		RMIUtils.mkdirs(_sca.getOutDir());
		RMIUtils.mkdirs(_sca.getOutScatterDir());
		RMIUtils.mkdirs(_sca.getTmpDir());
		RMIUtils.mkdirs(_sca.getLocalGatherDir());
		RMIUtils.mkdirsForFile(_sca.getOutAbox());
		RMIUtils.mkdirsForFile(_sca.getOutTbox());
		RMIUtils.mkdirsForFile(_sca.getOutConsolidated());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		_log.log(Level.INFO, "...connecting to peers...");

		try {
			_rmic = new RMIClient<RMIConsolidationInterface>(_servers, this, stubName);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_log.log(Level.INFO, "Connected.");

		_log.log(Level.INFO, "Connected in "+(System.currentTimeMillis() - b4)+" ms.");
	}

	public int getServerID(){
		return _serverID;
	}

	public RemoteInputStream extractTbox() throws RemoteException {
		_log.log(Level.INFO, "Extracting tbox from "+_sca.getIn()+" on server "+_serverID);
		ResetableIterator<Node[]> ri = null;
		FileInput fi = null;

		FileRedirects r = null;
		boolean auth = true;
		if(_sca.getRedirects()!=null){
			try{
				_log.log(Level.INFO, "Reading redirects from "+_sca.getRedirects()+" gz:"+_sca.getGzRedirects()+" on server "+_serverID);
				r = FileRedirects.createCompressedFileRedirects(_sca.getRedirects(), _sca.getGzRedirects());
				_log.log(Level.INFO, "Loaded "+r.size()+" redirects on server "+_serverID);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error reading redirects file "+_sca.getRedirects()+" gz:"+_sca.getGzRedirects()+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error reading redirects file "+_sca.getRedirects()+" gz:"+_sca.getGzRedirects()+" on server "+_serverID+"\n"+e);
			}
		} else{
			auth = false;
		}

		Rules rs = null;
		RedirectsAuthorityInspector rai = null;
		if(auth){
			rai = new RedirectsAuthorityInspector(r);
			rs = new Rules(_sca.getTboxExtractRules(), rai);
			rs.setAuthoritative();
		} else{
			rs = new Rules(_sca.getTboxExtractRules());
		}

		try{
			if(_sca.getGzIn()){
				fi = new NxGzInput(new File(_sca.getIn()));
			} else{
				fi = new NxInput(new File(_sca.getIn()));
			}
			ri = new FixCardinalityValues(fi);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
		}

		Callback cs = null;
		OutputStream os = null;
		try{
			os = new FileOutputStream(_sca.getOutTbox());
			os = new GZIPOutputStream(os);
			cs = new CallbackNxOutputStream(os);
			TboxExtractor.extractReducedTbox(ri, rs.getTboxRules(), cs, rai, _sca.getHandleCollections());
			os.close();
			fi.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating tbox file "+_sca.getOutTbox()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating tbox file "+_sca.getOutTbox()+" on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sca.getOutTbox())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
			_log.log(Level.INFO, "...finished extracting tbox from "+_sca.getIn()+" on server "+_serverID);
		}
	}

	public RemoteInputStream extractStatements(Collection<Statement> extract) throws RemoteException {
		return extractStatements(extract, null, null);
	}

	public RemoteInputStream extractStatements(Collection<Statement> extract, String tbox, Rule[] rules) throws RemoteException {
		_log.log(Level.INFO, "Extracting statements from "+_sca.getIn()+" on server "+_serverID);
		FileInput nxp = null;
		try{
			if(_sca.getGzIn()){
				nxp = new NxGzInput(new File(_sca.getIn()));
			} else{
				nxp = new NxInput(new File(_sca.getIn()));
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
		}

		Reasoner reasoner = null;
	
		if(tbox!=null && rules!=null){
			Rules rulez = new Rules(rules);
			LinkedRuleIndex<Rule> ruls = null;
			try{
				FileInput tboxIn = new NxaGzInput(new File(tbox),4);

				ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);

				Rules abox = new Rules(rulez.getAboxRules());
				abox.setAuthoritative();

				CallbackDummy cb = new CallbackDummy();

				ruls = MasterReasoner.buildTbox(fwiter, abox, cb, true);
				ruls.freeResources();

				tboxIn.close();
			}catch(Exception e){
				_log.log(Level.SEVERE, "Error loading tbox file "+tbox+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error loading tbox file "+tbox+" on server "+_serverID+"\n"+e);
			}

			ReasonerSettings rset = new ReasonerSettings();
			rset.setSkipTBox(true);
			rset.setSkipAxiomatic(true);
			rset.setPrintContexts(true);
			rset.setUseAboxRuleIndex(true);

			CallbackDummy cs = new CallbackDummy();
			ReasonerEnvironment re = new ReasonerEnvironment(nxp, cs);
			re.setAboxRuleIndex(ruls);

			reasoner = new Reasoner(rset, re);
		}

		PatternIndex bufferPI = new PatternIndex(extract);

		Callback csBuf = null;
		OutputStream osBuf = null;
		try{
			osBuf = new FileOutputStream(_sca.getOutAbox());
			if(_sca.getGzOutAbox())
				osBuf = new GZIPOutputStream(osBuf);
			csBuf = new CallbackNxOutputStream(osBuf);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating buffer file "+_sca.getOutAbox()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating buffer file "+_sca.getOutAbox()+" on server "+_serverID+"\n"+e);
		}

		long countBuf = 0, countIn = 0, inf = 0, countBufInf = 0, bl = 0, blInf = 0;
		Count<Node> blacklist = new Count<Node>();
		Count<Nodes> blacklistPO = new Count<Nodes>();
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			Statement triple = new Statement(next[0], next[1], next[2]);
			countIn++;

			if(countIn%TICKS==0){
				_log.info("Read "+countIn+" input and "+inf+" and inferred and written "+countBuf+" from input and "+countBufInf+" from inferred...");
			}
			
			//thanks parser/crawler bug
			if(triple.predicate.equals(FOAF.PRIMARYTOPIC)){
				continue;
			}
			if(bufferPI.isRelevant(triple)){
				if(IFP_BLACKLIST_HS.contains(triple.object) || triple.object.toString().isEmpty()){
					blacklist.add(triple.object);
					blacklistPO.add(new Nodes(triple.predicate, triple.object));
					bl++;
				} else if(IFP_BLACKLIST_HS.contains(triple.subject)){
					blacklist.add(triple.subject);
					blacklistPO.add(new Nodes(triple.predicate, triple.subject));
					bl++;
				} else{
					countBuf++;
					csBuf.processStatement(next);
				}
			} 
			
			if(reasoner!=null){
				HashSet<Statement> out = new HashSet<Statement>();
				out.add(triple);
				try{
					reasoner.reasonAbox(triple, null, out);
				} catch(InconsistencyException ie){
					_log.severe(ie.getMessage());
				}
			
				for(Statement s:out){
					inf++;
					if(bufferPI.isRelevant(s)){
						if(IFP_BLACKLIST_HS.contains(s.object) || s.object.toString().isEmpty()){
							blacklist.add(s.object);
							blacklistPO.add(new Nodes(s.predicate, s.object));
							blInf++;
						}  else if(IFP_BLACKLIST_HS.contains(s.subject)){
							blacklist.add(s.subject);
							blacklistPO.add(new Nodes(s.predicate, s.subject));
							bl++;
						} else{
							countBufInf++;
							if(next.length>3){
								csBuf.processStatement(appendContext(s.toNodeArray(),next[3]));
							}
						}
					} 
				}
			}
		}

		_log.info("Read "+countIn+" input and "+inf+" inferred and written "+countBuf+" from input and "+countBufInf+" from inferred...");
		_log.info("Found "+bl+" blacklisted from input and "+blInf+" blacklisted from inferred...");
		_log.info("Blacklist values");
		blacklist.printOrderedStats(_log, Level.INFO);
		_log.info("Blacklist pred-values");
		blacklistPO.printOrderedStats(_log, Level.INFO);

		try{
			nxp.close();
			osBuf.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing file on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sca.getOutAbox())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
			_log.log(Level.INFO, "...finished extracting patterns from "+_sca.getIn()+" on server "+_serverID);
		}
	}
	
	public Node[] appendContext(Node[] triple, Node context){
		Node[] quad = new Node[triple.length+1];
		System.arraycopy(triple, 0, quad, 0, triple.length);
		quad[triple.length] = context;
		return quad;
	}
	
	public void sort() throws RemoteException {
		NxParser.DEFAULT_PARSE_DTS = false;
		if(_sorting){
			_log.severe("Already sorting!!");
			return;
		}
		_sorting = true;
		String sorted = _sca.getOutDir()+"/"+SORTED_S_FILE;
		
		sort(_sca.getIn(), _sca.getGzIn(), _sca.getTmpDir(), sorted);
		
		_sorted = sorted;
		_sorting = false;
	}

	private static void sort(String input, boolean gz, String tmpdir, String output) throws RemoteException {
		sort(input, gz, tmpdir, output, NodeComparator.NC);
	}

	private static void sort(String input, boolean gz, String tmpdir, String output, Comparator<Node[]> nc) throws RemoteException {
		_log.info("Sorting input file "+input+"...");
		long b4 = System.currentTimeMillis();

		NxParser nxp = null;
		InputStream is = null;
		try{
			is = new FileInputStream(input);
			if(gz){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+input+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+input+"\n"+e);
		}

		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(output));
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+output+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+output+"\n"+e);
		}

		Callback cb = new CallbackNxOutputStream(os);

		SortArgs sa = new SortArgs(nxp);
		sa.setTicks(TICKS);
		sa.setTmpDir(tmpdir);
		sa.setComparator(nc);

		SortIterator si = null;
		try{
			si = new SortIterator(sa);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error sorting file "+input+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error sorting file "+input+"\n"+e);
		}

		while(si.hasNext()){
			cb.processStatement(si.next());
		}

		try{
			is.close();
			os.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files.\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files.\n"+e);
		}

		_log.info("...file sorted in "+(System.currentTimeMillis()-b4)+" ms. Found "+si.duplicates()+" duplicates from "+si.count()+" triples.");
	}
	
	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException {
		_log.info("Opening remote outputstream to file "+filename+"...");
		filename = RMIUtils.getLocalName(filename);
		RMIUtils.mkdirsForFile(filename);

		long b4 = System.currentTimeMillis();

		RemoteOutputStreamServer ostream = null;
		try{

			ostream = new SimpleRemoteOutputStream(new BufferedOutputStream(
					new FileOutputStream(filename)));

			synchronized(this){
				_toGatherFn.add(filename);
			}

			_log.info("...outputstream to file "+filename+" opened in "+(System.currentTimeMillis()-b4)+" ms.");
			return ostream;		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output stream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output stream on server "+_serverID+"\n"+e);
		}
	}

	public void consolidate(String sameAs) throws RemoteException{
//		_log.info("Sorting file...");
//		String sortedS = _sca.getOutDir()+"/"+SORTED_S_FILE;
//		sort(_sca.getIn(), _sca.getGzIn(), _sca.getTmpDir(), sortedS);
		
		if(_sorted==null){
			if(_sorting == true){
				//wait ten seconds
				while(_sorted!=null && _sorting){
					_log.info("Waiting for sort to finish...");
					synchronized(this){
						try {
							this.wait(10000);
						} catch (InterruptedException e) {
							throw new RemoteException("Error waiting on server "+_serverID+".\n"+e);
						}
					}
				}
			} else{
				sort();
			}
		}
		
		_log.info("Consolidating file...");
		long b4 = System.currentTimeMillis();

		String cons_s = _sca.getOutDir()+"/"+CONS_S_FILE;
		Callback cb = null;
		OutputStream os = null;
		InputStream is = null, sais = null;
		NxParser nxp = null, sanxp = null;

		try{
			is = new GZIPInputStream(new FileInputStream(_sorted));
			nxp = new NxParser(is);

			sais = new GZIPInputStream(new FileInputStream(sameAs));
			sanxp = new NxParser(sais);

			os = new GZIPOutputStream(new FileOutputStream(cons_s));
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening files on server "+_serverID+"\n"+e);
		} 

		rewrite(nxp, sanxp, cb, S, HandleNode.REWRITE);
		long t2 = System.currentTimeMillis();
		_log.info("...consolidated subjects of data in "+(t2-b4)+" ms -- total: "+(b4-t2));

		try{
			os.close();
			is.close();
			sais.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files on server "+_serverID+"\n"+e);
		} 

		String cons_o = _sca.getOutDir() +"/"+ CONS_O_FILE;
		cb = null; os = null; 
		is = null; nxp = null;  
		sais = null; sanxp = null;

		_log.info("...sorting output by O.");

		String sorted_o = _sca.getOutDir() +"/"+ SORTED_O_FILE;

		NodeComparator nc_o = new NodeComparator(O_ORDER);
		sort(cons_s, true, _sca.getTmpDir(), sorted_o, nc_o);
		long t3 = System.currentTimeMillis();
		_log.info("...sorted data by object in "+(t3-t2)+" ms -- total "+(t3-b4));

		try{
			is = new GZIPInputStream(new FileInputStream(sorted_o));
			nxp = new NxParser(is);

			sais = new GZIPInputStream(new FileInputStream(sameAs));
			sanxp = new NxParser(sais);

			os = new GZIPOutputStream(new FileOutputStream(cons_o));
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening files on server "+_serverID+"\n"+e);
		} 

		rewrite(nxp, sanxp, cb, O, HandleNode.BUFFER);
		long t4 = System.currentTimeMillis();
		_log.info("...consolidated objects of data in "+(t4-t3)+" ms -- total "+(t4-b4));


		try{
			os.close();
			is.close();
			sais.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files on server "+_serverID+"\n"+e);
		} 
		_log.info("...finished consolidation in total "+(t4-b4)+" ms");
	}

	public static void rewrite(Iterator<Node[]> data, Iterator<Node[]> sameAs, Callback cb, int pos, HandleNode rdfType) throws RemoteException {
		rewrite(data, sameAs, cb, pos, rdfType, HandleNode.REWRITE);
	}
	
	public static void rewrite(Iterator<Node[]> data, Iterator<Node[]> sameAs, Callback cb, int pos, HandleNode rdfTypeF, HandleNode sameasF) throws RemoteException {
		_log.info("Consolidating file...");
		long b4 = System.currentTimeMillis();

		ConsolidationIterator ci = new ConsolidationIterator(data, sameAs, sameasF, rdfTypeF, pos, true);

		while(ci.hasNext()){
			cb.processStatement(ci.next());
		}

		long t1 = System.currentTimeMillis();

		_log.info("...consolidated "+(t1-b4)+" ms. Scanned "+ci.count()+" stmts. Filtered "+ci.filtered()+" stmts. Rewrote "+ci.rewrittenStmts()+" statements. Rewrote "+ci.rewrittenIDs()+" ids.");
	}


	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIConsolidationServer rmi = new RMIConsolidationServer();

		RMIConsolidationInterface stub = (RMIConsolidationInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		;
	}
	
	public static class FixCardinalityValues implements ResetableIterator<Node[]>{
		ResetableIterator<Node[]> _in;
		
		public FixCardinalityValues(ResetableIterator<Node[]> in){
			_in = in;
		}

		public void reset() {
			_in.reset();
		}

		public boolean hasNext() {
			return _in.hasNext();
		}

		public Node[] next() {
			Node[] next = _in.next();
			if(next[1].equals(OWL.CARDINALITY) || next[1].equals(OWL.MAXCARDINALITY)){
				if(next[2] instanceof Literal){
					if(next[2].toString().equals("1")){
						next[2] = new Literal(Integer.toString(1), XSD.NONNEGATIVEINTEGER);
					}
				}
			}
			return next;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
		
	}
}
