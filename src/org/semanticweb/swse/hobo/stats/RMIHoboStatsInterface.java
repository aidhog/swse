package org.semanticweb.swse.hobo.stats;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

import org.semanticweb.hobo.stats.PerPLDNamingStats.PerPLDStatsResults;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIHoboStatsInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveHoboStatsArgs saa, String stubName) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException;
	
	public void scatterTriples(int[] order, Set<Node> ignorePreds) throws RemoteException;
	
	public void aggregateTriples(int[] order, String out) throws RemoteException;
	
	public Map<String,PerPLDStatsResults> stats(String spoc, String opsc) throws RemoteException;
}
