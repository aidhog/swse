package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;
import org.semanticweb.swse.econs.ercons.SlaveConsolidationArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationInitThread extends VoidRMIThread {
	private int _server;
	private RMIConsolidationInterface _stub;
	private RMIRegistries _servers;
	private SlaveConsolidationArgs _sca;
	private String _stubName;

	public RemoteConsolidationInitThread(RMIConsolidationInterface stub, int server, RMIRegistries servers, SlaveConsolidationArgs sca, String stubName){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sca = sca;
		_stubName = stubName;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sca, _stubName);
	}
}
