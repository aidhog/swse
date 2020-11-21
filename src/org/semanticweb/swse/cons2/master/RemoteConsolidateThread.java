package org.semanticweb.swse.cons2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidateThread extends VoidRMIThread {
	private RMIConsolidationInterface _stub;

	public RemoteConsolidateThread(RMIConsolidationInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.consolidate();
	}
}
