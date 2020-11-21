package org.semanticweb.swse.ann.agg.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateInterface;
import org.semanticweb.swse.ann.agg.SlaveAnnAggregateArgs;

/**
 * Thread to run init method on a remote server
 * @author aidhog
 *
 */
public class RemoteAggInitThread extends VoidRMIThread {
	private int _server;
	private RMIAnnAggregateInterface _stub;
	private RMIRegistries _servers;
	private SlaveAnnAggregateArgs _sra;
	private String _stubName;
	
	public RemoteAggInitThread(RMIAnnAggregateInterface stub, int server, RMIRegistries servers, SlaveAnnAggregateArgs sra, String stubName){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sra = sra;
		_stubName = stubName;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sra, _stubName);
	}
}
