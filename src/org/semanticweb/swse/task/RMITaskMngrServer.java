package org.semanticweb.swse.task;
//import java.rmi.RemoteException;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.rank.RMIRankingInterface;
import org.semanticweb.swse.rank.RMIRankingServer;



/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMITaskMngrServer implements RMITaskMngrInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6373272117534129172L;

	private final static Logger _log = Logger.getLogger(RMITaskMngrServer.class.getSimpleName());

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	public RMITaskMngrServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers) throws RemoteException {
		_log.log(Level.INFO, "Initialising server "+serverId+".");

		_serverID = serverId;
		
		_servers = servers;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "...initialised");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public void bind(String hostname, int port, String name, RMIInterface r, boolean startRegistry) throws RemoteException {
    	if(startRegistry){
    		_log.info("Starting registry on port "+port);
    		RMIUtils.startRMIRegistry(port);
    	}
    	
    	_log.info("Binding "+name+" on port "+port+" with hostname "+hostname);
    	
    	Remote stub = UnicastRemoteObject.exportObject(r, 0);
    	
	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
    	try{
    		registry.bind(name, stub);
    	} catch(AlreadyBoundException abe){
    		_log.severe("Registry already bound on server "+_serverID+"\n"+abe.getMessage());
    		abe.printStackTrace();
    		throw new RemoteException("Registry already bound on server "+_serverID+"\n"+abe.getMessage());
    	}
		long b4 = System.currentTimeMillis();
		
		_log.info("...registry bound in "+(System.currentTimeMillis()-b4)+" ms.");
	}

//	public void bindConsolidation(String hostname, int port, String name,
//			String log) throws RemoteException {
//		_log.info("Binding consolidation...");
//		long b4 = System.currentTimeMillis();
//		
//		try{
//			RMIConsolidationServer.startRMIServer(hostname, port, name);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error binding consolidation interface on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error binding consolidation interface on server "+_serverID+"\n"+e);
//		}
//		
//		try{
//			setLogger(log);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating log file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating log file on server "+_serverID+"\n"+e);
//		}
//		
//		_log.info("...consolidation bound in "+(System.currentTimeMillis()-b4)+" ms.");
//		
//	}
//
//	public void bindIndexing(String hostname, int port, String name, String log)
//			throws RemoteException {
//		_log.info("Binding indexing...");
//		long b4 = System.currentTimeMillis();
//		
//		try{
//			RMIIndexerServer.startRMIServer(hostname, port, name);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error binding indexing interface on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error binding indexing interface on server "+_serverID+"\n"+e);
//		}
//		
//		try{
//			setLogger(log);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating log file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating log file on server "+_serverID+"\n"+e);
//		}
//		
//		_log.info("...indexing bound in "+(System.currentTimeMillis()-b4)+" ms.");
//	}
//
//	public void bindLucene(String hostname, int port, String name, String log)
//			throws RemoteException {
//		// TODO Auto-generated method stub
//		_log.info("Binding lucene...");
//		long b4 = System.currentTimeMillis();
//		
//		try{
//			RMILuceneServer.startRMIServer(hostname, port, name);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error binding lucene interface on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error binding lucene interface on server "+_serverID+"\n"+e);
//		}
//		
//		try{
//			setLogger(log);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating log file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating log file on server "+_serverID+"\n"+e);
//		}
//		
//		_log.info("...lucene bound in "+(System.currentTimeMillis()-b4)+" ms.");
//	}
//
//	public void bindRanking(String hostname, int port, String name, String log)
//			throws RemoteException {
//		_log.info("Binding ranking...");
//		long b4 = System.currentTimeMillis();
//		
//		try{
//			RMIRankingServer.startRMIServer(hostname, port, name);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error binding ranking interface on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error binding ranking interface on server "+_serverID+"\n"+e);
//		}
//		
//		try{
//			setLogger(log);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating log file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating log file on server "+_serverID+"\n"+e);
//		}
//		
//		_log.info("...ranking bound in "+(System.currentTimeMillis()-b4)+" ms.");
//	}
//
//	public void bindReasoning(String hostname, int port, String name, String log)
//			throws RemoteException {
//		_log.info("Binding reasoning...");
//		long b4 = System.currentTimeMillis();
//		
//		try{
//			RMIReasonerServer.startRMIServer(hostname, port, name);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error binding reasoning interface on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error binding reasoning interface on server "+_serverID+"\n"+e);
//		}
//		
//		try{
//			setLogger(log);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error creating log file on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error creating log file on server "+_serverID+"\n"+e);
//		}
//		
//		_log.info("...reasoning bound in "+(System.currentTimeMillis()-b4)+" ms.");
//	}

	public void unbind(String hostname, int port, String name) throws RemoteException {
		_log.info("Unbinding "+name+" on port "+port+" with hostname "+hostname);
		Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
    	try{
    		registry.unbind(name);
    	} catch(Exception e){
			_log.log(Level.SEVERE, "Error unbinding interface "+name+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error unbinding interface "+name+" on server "+_serverID+"\n"+e);
		}
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMITaskMngrServer rmi = new RMITaskMngrServer();
    	
    	RMITaskMngrInterface stub = (RMITaskMngrInterface) UnicastRemoteObject.exportObject(rmi, 0);

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
}
