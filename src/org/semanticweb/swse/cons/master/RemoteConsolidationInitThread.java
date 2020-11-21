package org.semanticweb.swse.cons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cons.RMIConsolidationInterface;
import org.semanticweb.swse.cons.SlaveConsolidationArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationInitThread extends VoidRMIThread {
	private int _server;
	private RMIConsolidationInterface _stub;
	private SlaveConsolidationArgs _sca;
	private RMIRegistries _servers;

	public RemoteConsolidationInitThread(RMIConsolidationInterface stub, int server, RMIRegistries servers, SlaveConsolidationArgs sca){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sca = sca;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sca);
	}
}
