package org.semanticweb.swse.cons;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.ontologycentral.ldspider.http.Headers;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIConsolidationServer implements RMIConsolidationInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2708062661086563504L;
	private final static Logger _log = Logger.getLogger(RMIConsolidationServer.class.getSimpleName());
	public final static String SAME_AS_FILE = "sameas.nx.gz";
	public final static String OUTPUT_FILE = "data.cons.nq.gz";

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveConsolidationArgs _sca;

	public RMIConsolidationServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveConsolidationArgs sca) throws RemoteException {
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
		
		RMIUtils.mkdirsForFile(sca.getOut());
		RMIUtils.mkdirsForFile(sca.getSameAsOut());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public RemoteInputStream extractSameAs() throws RemoteException {
		_log.info("Starting same-as extraction from "+_sca.getIn()+" to "+_sca.getSameAsOut());
		
		NxParser nxp = null;
		InputStream is = null;
		
		try{
			is = new FileInputStream(_sca.getIn());
			if(_sca.getGzIn()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
		}

		Callback cs = null;
		OutputStream os = null;
		int saCount = 0;
		try{
			os = new GZIPOutputStream(new FileOutputStream(_sca.getSameAsOut()));
			cs = new CallbackNxOutputStream(os);
			saCount = extractSameAs(nxp, cs);
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating sameas file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating sameas file on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sca.getSameAsOut())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			
			_log.info("..."+saCount+" same-as relations extracted. Exporting results.");
			
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
		}
		
		
	}
	
	public int[] consolidate(SameAsIndex sai, int[] els) throws RemoteException{
		NxParser nxp = null;
		InputStream is = null;
		
		_log.info("Starting consolidation of "+_sca.getIn()+" to "+_sca.getOut()+".");
		
		try{
			is = new FileInputStream(_sca.getIn());
			if(_sca.getGzIn()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sca.getIn()+" on server "+_serverID+"\n"+e);
		}
		
		Callback cs = null;
		OutputStream os = null;
		int[] stats = null;
		try{
			os = new FileOutputStream(_sca.getOut());
			if(_sca.getGzOut())
				os = new GZIPOutputStream(os);
			cs = new CallbackNxOutputStream(os);
			stats = consolidate(nxp, sai, els, cs);
			sai.writeSameAs(cs, _servers, _serverID);
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error writing consolidated file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error writing consolidated file on server "+_serverID+"\n"+e);
		}
		_log.info("Finished consolidation, rewritten "+stats[0]+" identifiers from "+stats[3]+" possible ("+(double)stats[0]/(double)stats[3]+") with "+stats[6]+" ids in some equivalence class.");
		_log.info("Rewritten "+stats[1]+" subject identifiers from "+stats[4]+" possible ("+(double)stats[1]/(double)stats[4]+") with "+stats[7]+" ids in some equivalence class..");
		_log.info("Rewritten "+stats[2]+" object identifiers from "+stats[5]+" possible ("+(double)stats[2]/(double)stats[5]+") with "+stats[8]+" ids in some equivalence class..");
		return stats;
		
	}

	private int extractSameAs(Iterator<Node[]> nxp, Callback cs) {
		Node[] next;
		int count = 0;
		while(nxp.hasNext()){
			next = nxp.next();
			if(next[1].equals(OWL.SAMEAS)){
				cs.processStatement(new Node[]{next[0], next[2]});
				count++;
			}
		}
		return count;
	}
	
	private int[] consolidate(Iterator<Node[]> nxp, SameAsIndex sai, int[] els, Callback cs) {
		Node[] next;
		Node pivot;
		
		int rewritten = 0, rewrittenO = 0, rewrittenS = 0;
		int possible = 0, possibleO = 0, possibleS = 0;
		int insai = 0, insaiO = 0, insaiS = 0;
		while(nxp.hasNext()){
			next = nxp.next();
			if(next[1].equals(OWL.SAMEAS)){
				continue;
			}
			if(!next[1].equals(Headers.HEADERINFO)) for(int i:els){
				if(i == 2 && next[1].equals(RDF.TYPE)){
					continue;
				}
				
				if(i==0){
					possibleS++;
				} else if(i==2){
					possibleO++;
				}
				possible++;
				
				pivot = sai.getPivot(next[i]);
				
				
				if(pivot!=next[i]){
					insai++;
					if(i==0){
						insaiS++;
					} else if(i==2){
						insaiO++;
					}
					if(!pivot.equals(next[i])){
						rewritten++;
						if(i==0){
							rewrittenS++;
						} else if(i==2){
							rewrittenO++;
						}
					}
					next[i] = pivot;
				}
			}
			cs.processStatement(next);
		}
		
		return new int[]{rewritten, rewrittenS, rewrittenO, possible, possibleS, possibleO, insai, insaiS, insaiO};
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
		_sca = null;
	}
}
