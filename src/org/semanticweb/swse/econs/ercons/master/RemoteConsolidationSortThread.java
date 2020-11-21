package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationSortThread extends VoidRMIThread {
	private RMIConsolidationInterface _stub;

	public RemoteConsolidationSortThread(RMIConsolidationInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.sort();
	}
}
