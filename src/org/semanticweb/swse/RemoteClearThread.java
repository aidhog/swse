package org.semanticweb.swse;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteClearThread extends VoidRMIThread {
	private RMIInterface _stub;

	public RemoteClearThread(RMIInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.clear();
	}
	
	
}
