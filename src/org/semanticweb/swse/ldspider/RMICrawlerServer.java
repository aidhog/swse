package org.semanticweb.swse.ldspider;
//import java.rmi.RemoteException;
import java.io.IOException;
import java.net.URI;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.remote.RemoteCrawler;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerSetup;
import org.semanticweb.swse.ldspider.remote.utils.PersistentRedirects;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.yars.nx.Resource;


/**
 * Takes calls from the stub and translates into crawler actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering URIs.
 * 
 * @author aidhog
 */
public class RMICrawlerServer implements RMICrawlerInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1961523008474051061L;

	private final static Logger _log = Logger.getLogger(RMICrawlerServer.class.getSimpleName());
	
	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	private transient RMIClient<RMICrawlerInterface> _rmic;
	private transient RemoteCrawler _crawler;
	private transient RemoteCrawlerSetup _rcs;
	private transient PersistentRedirects _pr;
	
	public RMICrawlerServer() throws IOException, ClassNotFoundException{
		;
	}
	
	public void init(int serverId, RMIRegistries servers, RemoteCrawlerSetup rcs) throws RemoteException{
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		
		_log.log(Level.INFO, "...creating crawler...");
		_rcs = rcs;
		try {
			_crawler = rcs.createCrawler();
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up remote crawler on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up remote crawler on server "+serverId+"\n"+e);
		}
		_pr = rcs.getRedirects();
		_log.log(Level.INFO, "...created crawler...");
		
		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
	
		_log.log(Level.INFO, "...connecting to peers...");
		try {
			_rmic = new RMIClient<RMICrawlerInterface>(_servers, this, RMICrawlerConstants.DEFAULT_STUB_NAME);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");
		
		_log.log(Level.INFO, "Connected.");
	}
	
	//0
	public void addSeeds(Collection<String> uris) throws RemoteException {
		_log.log(Level.INFO, uris.size()+" seed URIs received from master server.");
		try {
			_crawler.addAllFrontier(uris);
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error accessing on-disk queue for remote crawler on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error accessing on-disk queue for remote crawler on server "+_serverID+"\n"+e);
		}
	}
	
	//1
	public int runRound(int targeturls) throws RemoteException{
		_log.log(Level.INFO, "Crawling round...");
	
		int result;
		try {
			result = _crawler.runRound(targeturls);
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error accessing on-disk queue for remote crawler on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error accessing on-disk queue for remote crawler on server "+_serverID+"\n"+e);
		}
		
		_log.log(Level.INFO, "...done.");
		return result;
	}
	
	//1a
	public int endRound(boolean poll) throws RemoteException{
		_log.log(Level.INFO, "Ending round...");
		int result = _crawler.endRound(poll);
		_log.log(Level.INFO, "...done.");
		return result;
	}
	
	//2
	public void scatter() throws RemoteException{
		_log.log(Level.INFO, "Scattering URIs from end of round...");
		Map<String, Integer> uris = _crawler.getNewURIs();
		Map<String, Integer>[] scatter = split(uris, _servers);
		
		Iterator<RMICrawlerInterface> stubIter = _rmic.getAllStubs().iterator();
		VoidRMIThread[] ibts = new VoidRMIThread[_rmic.getServers().getServerCount()];
		
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerAddAllThread(stubIter.next(), i, scatter[i], _crawler.getNewPldsStats(), _servers.thisServerId());
		}
		RMIUtils.startRandomOrder(ibts);

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			try{
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
			} catch(Exception e){
				_log.severe("Error scattering URIs to server "+i+":\n"+e.getMessage());
				e.printStackTrace();
				throw new RemoteException("Error scattering URIs to server "+i+":\n"+e.getMessage());
			}
			_log.log(Level.INFO, "...URIs sent to "+i+"...");
		}
		_log.log(Level.INFO, "...URIs scattered.");
		long idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering URIs "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering URIs "+(double)idletime/(double)(ibts.length)+"...");

		
//		for(int i=0; i<scatter.length; i++){
//			_rmic.getStub(i).addAll(scatter[i], _crawler.getNewPldsStats(), _servers.thisServerId());
//		}
		_log.log(Level.INFO, "...done.");
	}
	
	//3
	public void addAll(Map<String, Integer> uris, PldManager pldm,
			int fromServerID) throws RemoteException {
		_log.log(Level.INFO, "Adding "+ uris.size() +" URIs from server "+fromServerID+"...");
		_crawler.addAllFrontier(uris);
		_crawler.addRemoteStats(pldm);
		_log.log(Level.INFO, "...done.");
	}
	
	public static final Map<String,Integer>[] split(Map<String,Integer> uris, RMIRegistries servs){
		Map<String,Integer>[] scatter = new Map[servs.getServerCount()];
		
		for(int i=0; i<scatter.length; i++){
			scatter[i] = new Hashtable<String,Integer>();
		}
		
		for(Entry<String,Integer> e:uris.entrySet()){
			scatter[servs.getServerNo(new Resource(e.getKey()))].put(e.getKey(), e.getValue());
		}
		
		return scatter;
	}
	
	public Map<URI,URI> getRoundRedirects() throws RemoteException {
		Map<URI,URI> map  = _pr.getRoundRedirects().getMap();
		_pr.clearRound();
		return map;
	}
	
	public static final Collection<String>[] split(Collection<String> uris, RMIRegistries servs){
		Collection<String>[] scatter = new Collection[servs.getServerCount()];
		
		for(int i=0; i<scatter.length; i++){
			scatter[i] = new TreeSet<String>();
		}
		
		for(String u:uris){
			scatter[servs.getServerNo(new Resource(u))].add(u);
		}
		
		return scatter;
	}

		
	public int getServerID(){
		return _serverID;
	}

	public void finish() throws RemoteException {
		_log.log(Level.INFO, "Finishing up...");
		
		
		//ensures cleanup
		try{
			_crawler.close();
			_rcs.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing down remote crawler on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing down remote crawler on server "+_serverID+"\n"+e);
		}
		_log.log(Level.INFO, "...done.");
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMICrawlerServer rmi = new RMICrawlerServer();
    	
    	RMICrawlerInterface stub = (RMICrawlerInterface) UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		_crawler = null;
		_rcs = null;
		_pr = null;
	}

}
