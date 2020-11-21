package org.semanticweb.swse.index.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index.RMIIndexerInterface;
import org.semanticweb.swse.index.SlaveIndexerArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteIndexerInitThread extends VoidRMIThread {
	private int _server;
	private RMIIndexerInterface _stub;
	private RMIRegistries _servers;
	private SlaveIndexerArgs _sia;
	private String _stubName;

	public RemoteIndexerInitThread(RMIIndexerInterface stub, int server, RMIRegistries servers, SlaveIndexerArgs sia, String stubName){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sia = sia;
		_stubName = stubName;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sia, _stubName);
	}
}
