package org.semanticweb.swse.index.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index.RMIIndexerInterface;

/**
 * Thread to run index build on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteIndexerMakeIndexThread extends VoidRMIThread {
	private RMIIndexerInterface _stub;

	public RemoteIndexerMakeIndexThread(RMIIndexerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.makeIndex();
	}
}
