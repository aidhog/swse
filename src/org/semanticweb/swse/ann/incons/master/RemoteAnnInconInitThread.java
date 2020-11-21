package org.semanticweb.swse.ann.incons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.incon.RMIAnnInconInterface;
import org.semanticweb.swse.ann.incon.SlaveAnnInconArgs;
import org.semanticweb.swse.ann.reason.SlaveAnnReasonerArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteAnnInconInitThread extends VoidRMIThread {
	private int _server;
	private RMIAnnInconInterface _stub;
	private RMIRegistries _servers;
	private SlaveAnnInconArgs _sra;

	public RemoteAnnInconInitThread(RMIAnnInconInterface stub, int server, RMIRegistries servers, SlaveAnnInconArgs sra){
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
