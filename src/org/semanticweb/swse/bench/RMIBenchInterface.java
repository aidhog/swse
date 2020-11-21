package org.semanticweb.swse.bench;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMIBenchInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, String outdir) throws RemoteException;
	
	public void scatter(String infile, boolean gzip) throws RemoteException;
	
	public void gather(RemoteInputStream inFile) throws RemoteException;
	
	public RemoteInputStream gatherLocal(String inFile, boolean gzip) throws RemoteException;
	
	public void close() throws RemoteException;
	
}
