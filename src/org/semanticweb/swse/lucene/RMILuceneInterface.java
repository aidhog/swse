package org.semanticweb.swse.lucene;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMILuceneInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveLuceneArgs sla) throws RemoteException;
	
	public void buildLucene() throws RemoteException;
}
