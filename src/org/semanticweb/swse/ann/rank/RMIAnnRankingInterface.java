package org.semanticweb.swse.ann.rank;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIAnnRankingInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveAnnRankingArgs sra, String stubName) throws RemoteException;
	
	public RemoteInputStream sortByContext() throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename, boolean toGather) throws RemoteException;
	
	public RemoteInputStream[] extractGraph(String allContexts) throws RemoteException;
	
	public void rankAndScatterTriples(String allRanks) throws RemoteException;
	
	public void aggregateRanks() throws RemoteException;
}
