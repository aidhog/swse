package org.semanticweb.swse.ldspider;

import java.net.URI;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Map;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerSetup;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMICrawlerInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, RemoteCrawlerSetup rcs) throws RemoteException;
	
	public void addAll(Map<String,Integer> uris, PldManager pldm, int fromServerID) throws RemoteException;
	
	public void addSeeds(Collection<String> uris) throws RemoteException;
	
	public int runRound(int targeturis) throws RemoteException;
	
	public int endRound(boolean join) throws RemoteException;
	
	public Map<URI,URI> getRoundRedirects() throws RemoteException;
	
	public void scatter() throws RemoteException;
	
	public void finish() throws RemoteException;
}
