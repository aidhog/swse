package org.semanticweb.swse.ann.reason;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.engine.unique.UniquingAnnotationHashset;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.auth.RedirectsAuthorityInspector;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxInput;
import org.semanticweb.saorr.engine.output.NxOutput;
import org.semanticweb.saorr.engine.tbox.TboxExtractor;
import org.semanticweb.saorr.index.StatementStore;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.saor.utils.CheckAuthorityInspector;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIAnnReasonerServer implements RMIAnnReasonerInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4983859112724015479L;
	private final static Logger _log = Logger.getLogger(RMIAnnReasonerServer.class.getSimpleName());
	public final static String TBOX_FILE = "tbox.nq.gz";
	public final static String OUTPUT_FILE = "data.r.nq.gz";
	public final static String TEMP_DIR = "tmp";
	public final static int TICKS = 1000000;
	
	public final static Resource[] PREDS_COLLS = new Resource[]{
		OWL.ONEOF, OWL.UNIONOF, OWL.INTERSECTIONOF
	};

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveAnnReasonerArgs _sra;

	public RMIAnnReasonerServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveAnnReasonerArgs sra) throws RemoteException {
		try {
			RMIUtils.setLogFile(sra.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sra);
		
		_sra = sra;
		
		RMIUtils.mkdirsForFile(sra.getOutReasoned());
		RMIUtils.mkdirsForFile(sra.getOutTbox());
		
		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public RemoteInputStream extractTbox() throws RemoteException {
		_log.log(Level.INFO, "Extracting tbox from "+_sra.getTboxInput()+" on server "+_serverID);
		NxParser nxp = null;
		InputStream is = null;
		
		FileRedirects r = null;
		boolean auth = true;
		if(_sra.getRedirects()!=null){
			try{
				_log.log(Level.INFO, "Reading redirects from "+_sra.getRedirects()+" gz:"+_sra.getGzRedirects()+" on server "+_serverID);
				r = FileRedirects.createCompressedFileRedirects(_sra.getRedirects(), _sra.getGzRedirects());
				_log.log(Level.INFO, "Loaded "+r.size()+" redirects on server "+_serverID);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error reading redirects file "+_sra.getRedirects()+" gz:"+_sra.getGzRedirects()+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error reading redirects file "+_sra.getRedirects()+" gz:"+_sra.getGzRedirects()+" on server "+_serverID+"\n"+e);
			}
		} else{
			auth = false;
		}
		
		Rules rs = null;
		RedirectsAuthorityInspector rai = null;
		if(auth){
			rai = new RedirectsAuthorityInspector(r);
			rs = new Rules(_sra.getTboxExtractRules(), rai);
			rs.setAuthoritative();
		} else{
			rs = new Rules(_sra.getTboxExtractRules());
		}
		
		try{
			is = new FileInputStream(_sra.getTboxInput());
			if(_sra.getGzInTbox()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getTboxInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getTboxInput()+" on server "+_serverID+"\n"+e);
		}

		Callback cs = null;
		OutputStream os = null;
		try{
			os = new FileOutputStream(_sra.getOutTbox());
			os = new GZIPOutputStream(os);
			cs = new CallbackNxOutputStream(os);
			extractTbox(nxp, rs, rai, _sra.getHandleCollections(), cs);
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating tbox file "+_sra.getOutTbox()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating tbox file "+_sra.getOutTbox()+" on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sra.getOutTbox())));
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
			_log.log(Level.INFO, "...finished extracting tbox from "+_sra.getTboxInput()+" on server "+_serverID);
		}
	}

	public void reason(StatementStore tbox) throws RemoteException {
		FileInput fi = null;
		
		Rules rules = new Rules(_sra.getRules());
		Rule[] abox = rules.getAboxRules();
		
		CheckAuthorityInspector cai = new CheckAuthorityInspector();
		for(Rule rule:abox){
			if(_sra.getRedirects()!=null){
				rule.setAuthorityInspector(cai);
				rule.setAuthoritative();
			}
			rule.setPrintContexts();
		}
		
		try{
			if(_sra.getGzInAbox()){
				fi = new NxGzInput(new File(_sra.getAboxInput()));
			} else{
				fi = new NxInput(new File(_sra.getAboxInput()));
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getAboxInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getAboxInput()+" on server "+_serverID+"\n"+e);
		}

		Callback cb = null;
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(_sra.getOutReasoned()));
			cb = new NxOutput(new PrintWriter(new OutputStreamWriter(os)), true, false, false);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating reasoning output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating reasoning output file on server "+_serverID+"\n"+e);
		}
		
		ReasonerEnvironment re = new ReasonerEnvironment(fi, cb);
		re.setTBox(tbox);
		if(_sra.getRedirects()!=null){
			re.setAuthorityInspector(cai);
		}
		
		ReasonerSettings rs = new ReasonerSettings();
		if(_sra.getRedirects()!=null){
			rs.setAuthorativeReasoning(true);
		}
		rs.setFragment(abox);
		rs.setSkipTBox(true);
		rs.setSkipAxiomatic(true);
		
		
		Reasoner r = new Reasoner(rs, re);
		
		try{
			r.reason();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error reasoning on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error reasoning on server "+_serverID+"\n"+e);
		}
		
		try{
			os.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing reasoning output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing reasoning output file on server "+_serverID+"\n"+e);
		}
		
	}
	
	public void reason(LinkedRuleIndex<AnnotationRule<RankAnnotation>> aboxTemplateRuleIndex) throws RemoteException {
		FileInput fi = null;
		
		try{
			if(_sra.getGzInAbox()){
				fi = new NxGzInput(new File(_sra.getAboxInput()));
			} else{
				fi = new NxInput(new File(_sra.getAboxInput()));
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getAboxInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getAboxInput()+" on server "+_serverID+"\n"+e);
		}

		Callback cb = null;
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(_sra.getOutReasoned()));
			cb = new NxOutput(new PrintWriter(new OutputStreamWriter(os)), true, false, false);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating reasoning output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating reasoning output file on server "+_serverID+"\n"+e);
		}
		
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(fi, RankAnnotationFactory.SINGLETON, cb);
		re.setAboxRuleIndex(aboxTemplateRuleIndex);
		
		ReasonerSettings rs = new ReasonerSettings();
		rs.setSkipTBox(true);
		rs.setSkipAxiomatic(true);
		rs.setPrintContexts(true);
		rs.setUseAboxRuleIndex(true);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rs, re);
		
		try{
			r.reason();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error reasoning on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error reasoning on server "+_serverID+"\n"+e);
		}
		
		try{
			os.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing reasoning output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing reasoning output file on server "+_serverID+"\n"+e);
		}
		
	}
	
	private void extractTbox(Iterator<Node[]> nxp, Rules rules, RedirectsAuthorityInspector rai, boolean handleCollections, Callback cs) {
		TboxExtractor.extractReducedTbox(nxp, rules.getTboxRules(), cs, rai, true, TICKS);
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIAnnReasonerServer rmi = new RMIAnnReasonerServer();
    	
    	RMIAnnReasonerInterface stub = (RMIAnnReasonerInterface) UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}
	
	public void clear() throws RemoteException {
		_sra = null;
	}
}
