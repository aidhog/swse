package org.semanticweb.swse.econs.sim;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.swse.econs.sim.utils.Stats;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIEconsSimInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveEconsSimArgs saa, String stubName) throws RemoteException;
	
	public ArrayList<HashMap<Node,PredStats>> predicateStatistics() throws RemoteException;
	
	public ArrayList<Integer> generateSimilarity(ArrayList<HashMap<Node,PredStats>> predStats) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException;
	
	public void scatterSimilarity() throws RemoteException;
	
	public Stats<Double> aggregateSimilarity() throws RemoteException;
}
