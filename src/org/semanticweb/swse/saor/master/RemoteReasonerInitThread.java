package org.semanticweb.swse.saor.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.saor.RMIReasonerInterface;
import org.semanticweb.swse.saor.SlaveReasonerArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteReasonerInitThread extends VoidRMIThread {
	private int _server;
	private RMIReasonerInterface _stub;
	private RMIRegistries _servers;
	private SlaveReasonerArgs _sra;

	public RemoteReasonerInitThread(RMIReasonerInterface stub, int server, RMIRegistries servers, SlaveReasonerArgs sra){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sra = sra;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sra);
	}
}
