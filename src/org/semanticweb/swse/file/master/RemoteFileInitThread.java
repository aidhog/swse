package org.semanticweb.swse.file.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.file.RMIFileInterface;
import org.semanticweb.swse.file.SlaveFileArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteFileInitThread extends VoidRMIThread {
	private int _server;
	private RMIFileInterface _stub;
	private RMIRegistries _servers;
	private SlaveFileArgs _sla;

	public RemoteFileInitThread(RMIFileInterface stub, int server, RMIRegistries servers, SlaveFileArgs sla){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sla = sla;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sla);
	}
}
