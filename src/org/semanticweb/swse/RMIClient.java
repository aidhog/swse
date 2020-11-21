package org.semanticweb.swse;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A general purpose class for organising RMI stubs for multiple
 * servers.
 * 
 * @author aidhog
 *
 * @param <E> The type of stub (extends Remote)
 */
public class RMIClient<E extends RMIInterface> {
	private final static Logger _log = Logger.getLogger(RMIClient.class.getSimpleName());
	
	private ArrayList<E> _stubs;
	private RMIRegistries _servers;
	int _thisServer = -1;
	
	public RMIClient(RMIRegistries servers, String stubName) throws RemoteException, NotBoundException{
		_servers = servers;
		_stubs = new ArrayList<E>(servers.getServerCount());
		for(int i = 0; i<_servers.getServerCount(); i++){
			_log.log(Level.INFO, "...connecting to server "+i+" ("+_servers.getServer(i)+") on stub "+stubName+"...");
			RMIRegistry server = _servers.getServer(i);
			Registry registry = LocateRegistry.getRegistry(server.getServerUrl(), server.getPort());
			E stub = (E) registry.lookup(stubName);
			_stubs.add(stub);
			_log.log(Level.INFO, "...connected to server "+i+" ("+_servers.getServer(i)+") on stub "+stubName+"...");
		}
	}
	
	public RMIClient(RMIRegistries servers, E localStub, String stubName) throws RemoteException, NotBoundException{
		_servers = servers;
		_stubs = new ArrayList<E>(servers.getServerCount());
		for(int i = 0; i<_servers.getServerCount(); i++){
			RMIRegistry server = _servers.getServer(i);
			if(!server.equals(servers.thisServer())){
				_log.log(Level.INFO, "...connecting to server "+i+" ("+_servers.getServer(i)+")...");
				Registry registry = LocateRegistry.getRegistry(server.getServerUrl(), server.getPort());
				E stub = (E) registry.lookup(stubName);
				_stubs.add(stub);
			} else{
				_log.log(Level.INFO, "...using local stub for server "+i+" ("+_servers.getServer(i)+")...");
				_stubs.add(localStub);
			}
			_log.log(Level.INFO, "...connected to server "+i+" ("+_servers.getServer(i)+")...");
		}
	}
	
	/**
	 * Gets stub for server
	 * @param i Server ID number
	 * @return
	 */
	public E getStub(int i){
		return _stubs.get(i);
	}
	
	/**
	 * Uses hash to find stub for appropriate server
	 * @param o Object to hash to find server ID number
	 * @return
	 */
	public E getStub(Object o){
		return _stubs.get(_servers.getServerNo(o));
	}
	
	/**
	 * Get all stubs
	 * @return
	 */
	public ArrayList<E> getAllStubs(){
		return _stubs;
	}
	
	public RMIRegistries getServers(){
		return _servers;
	}
	
	/**
	 * Deallocate resources attached to the remote servers 
	 * @throws Exception 
	 */
	public void clear() throws Exception{
		RMIThread<? extends Object>[] ibts = new RMIThread[_stubs.size()];
		_log.log(Level.INFO, "Cleaning up servers...");
		
		Iterator<E> stubIter = _stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteClearThread(stubIter.next(), i);
			ibts[i].start();
		}

		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" cleared...");
		}
		_log.log(Level.INFO, "...remote servers cleared.");
		long idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on clearing "+idletime+"...");
		_log.info("Average idle time for co-ordination on clearing "+(double)idletime/(double)(ibts.length)+"...");
	}
}

