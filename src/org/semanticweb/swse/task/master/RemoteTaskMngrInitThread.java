package org.semanticweb.swse.task.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.task.RMITaskMngrInterface;

/**
 * Thread to run init method on a remote builder server
 * @author aidhog
 *
 */
public class RemoteTaskMngrInitThread extends VoidRMIThread {
	private int _server;
	private RMITaskMngrInterface _stub;
	private RMIRegistries _servers;

	public RemoteTaskMngrInitThread(RMITaskMngrInterface stub, int server, RMIRegistries servers){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers);
	}
}
