package org.semanticweb.swse.ann.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteAggregateRanksThread extends VoidRMIThread {
	private RMIAnnRankingInterface _stub;
	
	public RemoteAggregateRanksThread(RMIAnnRankingInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.aggregateRanks();
	}
}
