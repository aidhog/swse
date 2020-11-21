package org.semanticweb.swse.ann.agg.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteAggregateRanksThread extends VoidRMIThread {
	private RMIAnnAggregateInterface _stub;
	
	public RemoteAggregateRanksThread(RMIAnnAggregateInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.aggregateRanks();
	}
}
