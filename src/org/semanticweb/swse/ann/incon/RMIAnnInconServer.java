package org.semanticweb.swse.ann.incon;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.index.AnnotatedMapTripleStore;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyHandler;
import org.semanticweb.saorr.engine.ih.InconsistencyLogger;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxInput;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.incons.utils.ClearStatementStoreIterator;
import org.semanticweb.swse.ann.incons.utils.PatternIndex;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIAnnInconServer implements RMIAnnInconInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4983859112724015479L;
	private final static Logger _log = Logger.getLogger(RMIAnnInconServer.class.getSimpleName());
	public final static int TICKS = 1000000;
	
	public final static Resource[] PREDS_COLLS = new Resource[]{
		OWL.ONEOF, OWL.UNIONOF, OWL.INTERSECTIONOF
	};

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveAnnInconArgs _sia;

	public RMIAnnInconServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveAnnInconArgs sra) throws RemoteException {
		try {
			RMIUtils.setLogFile(sra.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sra);
		
		_sia = sra;
		
		RMIUtils.mkdirsForFile(sra.getOutData());
		RMIUtils.mkdirsForFile(sra.getOutInconsistencies());
		
		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public Count<Statement> getCardinalities(Collection<Statement> patterns)
			throws RemoteException {
		_log.log(Level.INFO, "Getting cardinalities from "+patterns.size()+" patterns on server "+_serverID);
		InputStream is = null;
		Iterator<Node[]> in = null;
		try{
			is = new FileInputStream(_sia.getInput());
			if(_sia.getGzIn()){
				is = new GZIPInputStream(is);
			}
			in = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
		}
		
		Count<Statement> cs = new Count<Statement>();
		PatternIndex pi = new PatternIndex(patterns);
		
		int c = 0;
		while(in.hasNext()){
			c++;
			if(c%TICKS==0){
				_log.info("Processed "+c+" input statements...");
			}
			Node[] next = in.next();
			Statement triple = new Statement(next[0], next[1], next[2]);
			Set<Statement> set = pi.getRelevantPatterns(triple);
			for(Statement p:set){
				cs.add(p);
			}
		}
		
		_log.log(Level.INFO, "...cardinalities done on server "+_serverID+". Read "+c+" input statements.");
		
		try{
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
		}
		
		return cs;
	}
	
	public RemoteInputStream extractStatements(Collection<Statement> extract) throws RemoteException {
		_log.log(Level.INFO, "Extracting statements from "+_sia.getInput()+" on server "+_serverID);
		NxParser nxp = null;
		InputStream is = null;
		try{
			is = new FileInputStream(_sia.getInput());
			if(_sia.getGzIn()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
		}
		
		PatternIndex bufferPI = new PatternIndex(extract);
		
		Callback csBuf = null;
		OutputStream osBuf = null;
		try{
			osBuf = new FileOutputStream(_sia.getOutData());
			if(_sia.getGzData())
				osBuf = new GZIPOutputStream(osBuf);
			csBuf = new CallbackNxOutputStream(osBuf);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating buffer file "+_sia.getOutData()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating buffer file "+_sia.getOutData()+" on server "+_serverID+"\n"+e);
		}
		
		long countBuf = 0, countIn = 0;
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			Statement triple = new Statement(next[0], next[1], next[2]);
			countIn++;
			
			if(countIn%TICKS==0){
				_log.info("Read "+countIn+" and written "+countBuf+"...");
			}
			if(bufferPI.isRelevant(triple)){
				countBuf++;
				csBuf.processStatement(next);
			} 
		}
		
		_log.info("Extracted "+countBuf+" from "+countIn+" input.");
		
		try{
			is.close();
			osBuf.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing file on server "+_serverID+"\n"+e);
		}
		
		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sia.getOutData())));
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
			_log.log(Level.INFO, "...finished extracting patterns from "+_sia.getInput()+" on server "+_serverID);
		}
	}

	public void reasonInconsistencies(LinkedRuleIndex<AnnotationRule<RankAnnotation>> aboxTemplateRuleIndex) throws RemoteException {
		FileInput fi = null;
		
		try{
			if(_sia.getGzIn()){
				fi = new NxGzInput(new File(_sia.getInput()));
			} else{
				fi = new NxInput(new File(_sia.getInput()));
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sia.getInput()+" on server "+_serverID+"\n"+e);
		}

		Callback cb = new CallbackDummy();
//		try{
//			cb = new NxOutput(new PrintWriter(System.out), true, false, false);
//		}catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating reasoning output file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating reasoning output file on server "+_serverID+"\n"+e);
//		}
		
		OutputStream os = null;
		try{
			os = new FileOutputStream(_sia.getOutInconsistencies());
			if(_sia.getGzInconsistencies()){
				os = new GZIPOutputStream(os);
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating inconsistency output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating inconsistency output file on server "+_serverID+"\n"+e);
		}
		
		InconsistencyHandler ih = new InconsistencyLogger(new PrintWriter(os));
		
		AnnotatedMapTripleStore<RankAnnotation> aBox = new AnnotatedMapTripleStore<RankAnnotation>();
		ResetableIterator<Node[]> input = new ClearStatementStoreIterator(fi, aBox);
		
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(input, RankAnnotationFactory.SINGLETON, cb);
		re.setAboxRuleIndex(aboxTemplateRuleIndex);
		re.setInconsistencyHandler(ih);
		re.setABox(aBox);
		
		ReasonerSettings rs = new ReasonerSettings();
		rs.setSkipTBox(true);
		rs.setSkipAxiomatic(true);
		rs.setPrintContexts(true);
		rs.setUseAboxRuleIndex(true);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rs, re);
		
		_log.info("Starting reasoning on server "+_serverID);
		try{
			r.reason();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error inconsistency reasoning on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error inconsistency reasoning on server "+_serverID+"\n"+e);
		}
		_log.info("...finished reasoning on server "+_serverID);
		
		try{
			os.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing inconsistency output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing inconsistency output file on server "+_serverID+"\n"+e);
		}
		
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIAnnInconServer rmi = new RMIAnnInconServer();
    	
    	RMIAnnInconInterface stub = (RMIAnnInconInterface) UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}
	
	public void clear() throws RemoteException {
		_sia = null;
	}
}
