package org.semanticweb.swse.ann.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteRankScatterTriplesThread extends VoidRMIThread {
	private RMIAnnRankingInterface _stub;
	private String _allRanks;
	
	public RemoteRankScatterTriplesThread(RMIAnnRankingInterface stub, int server, String allRanks){
		super(server);
		_stub = stub;
		_allRanks = allRanks;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.rankAndScatterTriples(_allRanks);
	}
}
