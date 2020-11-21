package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationThread extends VoidRMIThread {
	private RMIConsolidationInterface _stub;
	private String _sameas;

	public RemoteConsolidationThread(RMIConsolidationInterface stub, int server, String sameas){
		super(server);
		_stub = stub;
		_sameas = sameas;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.consolidate(_sameas);
	}
}
