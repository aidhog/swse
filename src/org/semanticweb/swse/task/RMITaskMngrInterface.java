package org.semanticweb.swse.task;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;


/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMITaskMngrInterface extends RMIInterface {
	public void init(int server, RMIRegistries servers) throws RemoteException;
	
	public void bind(String hostname, int port, String name, RMIInterface r, boolean startRegistry) throws RemoteException;
	
//	public void bindConsolidation(String hostname, int port, String name, String log) throws RemoteException;
//	public void bindRanking(String hostname, int port, String name, String log) throws RemoteException;
//	public void bindReasoning(String hostname, int port, String name, String log) throws RemoteException;
//	public void bindIndexing(String hostname, int port, String name, String log) throws RemoteException;
//	public void bindLucene(String hostname, int port, String name, String log) throws RemoteException;
	
	public void unbind(String hostname, int port, String name) throws RemoteException;
}
