package org.semanticweb.swse.econs.stats;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.econs.stats.RMIEconsStatsServer.StatsResults;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIEconsStatsInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveEconsStatsArgs saa, String stubName) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException;
	
	public void scatterTriples(int[] order, Set<Node> ignorePreds) throws RemoteException;
	
	public void aggregateTriples(int[] order, String out) throws RemoteException;
	
	public StatsResults stats(String spoc, String opsc, Set<Node> ignorePreds) throws RemoteException;
}
