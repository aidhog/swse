package org.semanticweb.swse.qp;
//import java.rmi.RemoteException;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;
import com.healthmarketscience.rmiio.SerialRemoteIteratorClient;
import com.healthmarketscience.rmiio.SerialRemoteIteratorServer;

/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIQueryServer implements RMIQueryInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4910109856415127245L;

	private final static Logger _log = Logger.getLogger(RMIQueryServer.class.getSimpleName());

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient QueryProcessor _qp;


	public RMIQueryServer() throws IOException, ClassNotFoundException{
		;
	}

	public void init(int serverId, RMIRegistries servers, String lucene, String spoc, String sparse) throws RemoteException {
		_log.log(Level.INFO, "Initialising server "+serverId+".");

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);

		try{
			_qp = new QueryProcessor(spoc, sparse, lucene);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error starting query-processor on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error starting query-processor on server "+_serverID+"\n"+e);
		}

		_log.log(Level.INFO, "Connected.");
	}

	public RemoteIterator<Node[]> simpleQuery(String keywordQ, int from, int to, String prefLang) throws RemoteException {
		Iterator<Node[]> iter = null;
		try{
			iter = _qp.getSnippets(keywordQ, from, to, prefLang);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error processing simple query "+keywordQ+" ("+from+":"+to+")  on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error processing simple query "+keywordQ+" ("+from+":"+to+")  on server "+_serverID+"\n"+e);
		}

		SerialRemoteIteratorClient<Node[]> stringClient = null;
		try{
			SerialRemoteIteratorServer<Node[]> stringServer =
				new SerialRemoteIteratorServer<Node[]>(iter);
				stringClient =
					new SerialRemoteIteratorClient<Node[]>(stringServer);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error creating remote iterator on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating remote iterator on server "+_serverID+"\n"+e);
		}

		return stringClient;
	}

	public ScoreDoc[] getKeywordHits(String keywordQ, int from, int to, String prefLang) throws RemoteException {
		ScoreDoc[] hits = null;
		try{
			hits = _qp.getDocs(keywordQ, from, to);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error processing simple query "+keywordQ+" ("+from+":"+to+")  on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error processing simple query "+keywordQ+" ("+from+":"+to+")  on server "+_serverID+"\n"+e);
		}

		return hits;
		
	}
	
	public RemoteIterator<Node[]> getSnippets(ScoreDoc[] hits, String prefLang) throws RemoteException {
		Iterator<Node[]> iter = null;
		try{
			iter = _qp.getSnippets(hits, prefLang);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error retrieving hits on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error retrieving hits on server on server "+_serverID+"\n"+e);
		}

		SerialRemoteIteratorClient<Node[]> stringClient = null;
		try{
			SerialRemoteIteratorServer<Node[]> stringServer =
				new SerialRemoteIteratorServer<Node[]>(iter);
				stringClient =
					new SerialRemoteIteratorClient<Node[]>(stringServer);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error creating remote iterator on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating remote iterator on server "+_serverID+"\n"+e);
		}

		return stringClient;
	}
	
	public RemoteIterator<Node[]> getEntities(Collection<Node> ns, String prefLang)
			throws RemoteException {
		Iterator<Node[]> iter = null;
		try{
			iter = _qp.getEntities(ns, prefLang);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error retrieving entities on server "+_serverID+"\n"+ns+"\n"+e.getMessage());
			e.printStackTrace();
			throw new RemoteException("Error retrieving entities on server on server "+_serverID+"\n"+ns+"\n"+e.getMessage());
		}

		SerialRemoteIteratorClient<Node[]> stringClient = null;
		try{
			SerialRemoteIteratorServer<Node[]> stringServer =
				new SerialRemoteIteratorServer<Node[]>(iter);
				stringClient =
					new SerialRemoteIteratorClient<Node[]>(stringServer);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error creating remote iterator on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating remote iterator on server "+_serverID+"\n"+e);
		}

		return stringClient;
	}

	public RemoteIterator<Node[]> getFocus(Node n) throws RemoteException {
		Iterator<Node[]> iter = null;
		try{
			iter = _qp.getFocus(n);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error retrieving focus on server "+_serverID+"\n"+n+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error retrieving focus on server on server "+_serverID+"\n"+n+"\n"+e);
		}

		SerialRemoteIteratorClient<Node[]> stringClient = null;
		try{
			SerialRemoteIteratorServer<Node[]> stringServer =
				new SerialRemoteIteratorServer<Node[]>(iter);
				stringClient =
					new SerialRemoteIteratorClient<Node[]>(stringServer);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error creating remote iterator on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating remote iterator on server "+_serverID+"\n"+e);
		}

		return stringClient;
	}

	public int getServerID(){
		return _serverID;
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIQueryServer rmi = new RMIQueryServer();

		RMIQueryInterface stub = (RMIQueryInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		_qp = null;
	}
}
