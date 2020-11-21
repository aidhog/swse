package org.semanticweb.swse.ann.agg;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIAnnAggregateInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveAnnAggregateArgs saa, String stubName) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename, boolean toGather) throws RemoteException;
	
	public void scatterTriples() throws RemoteException;
	
	public void aggregateRanks() throws RemoteException;
}
