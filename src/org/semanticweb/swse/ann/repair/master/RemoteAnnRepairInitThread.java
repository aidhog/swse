package org.semanticweb.swse.ann.repair.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.repair.RMIAnnRepairInterface;
import org.semanticweb.swse.ann.repair.SlaveAnnRepairArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteAnnRepairInitThread extends VoidRMIThread {
	private int _server;
	private RMIAnnRepairInterface _stub;
	private RMIRegistries _servers;
	private SlaveAnnRepairArgs _sra;

	public RemoteAnnRepairInitThread(RMIAnnRepairInterface stub, int server, RMIRegistries servers, SlaveAnnRepairArgs sra){
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
