package org.semanticweb.swse.index;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMIIndexerInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveIndexerArgs sia, String stubName) throws RemoteException;
	
	public void scatter(String[] infile, boolean[] gzip) throws RemoteException;
	
	public void gather(RemoteInputStream inFile) throws RemoteException;
	
	public void makeIndex() throws RemoteException;
	
}
