package org.semanticweb.swse.file;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMIFileInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveFileArgs sla) throws RemoteException;
	
	public void receiveFile(RemoteInputStream ris, String s) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String s) throws RemoteException;
}
