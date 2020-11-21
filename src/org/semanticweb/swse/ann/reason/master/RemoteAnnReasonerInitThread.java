package org.semanticweb.swse.ann.reason.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerInterface;
import org.semanticweb.swse.ann.reason.SlaveAnnReasonerArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteAnnReasonerInitThread extends VoidRMIThread {
	private int _server;
	private RMIAnnReasonerInterface _stub;
	private RMIRegistries _servers;
	private SlaveAnnReasonerArgs _sra;

	public RemoteAnnReasonerInitThread(RMIAnnReasonerInterface stub, int server, RMIRegistries servers, SlaveAnnReasonerArgs sra){
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
