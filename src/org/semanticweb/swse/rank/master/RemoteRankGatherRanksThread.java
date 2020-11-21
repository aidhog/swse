package org.semanticweb.swse.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.rank.RMIRankingInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteRankGatherRanksThread extends VoidRMIThread {
	private RMIRankingInterface _stub;
	private RemoteInputStream _ris; 
	
	public RemoteRankGatherRanksThread(RMIRankingInterface stub, int server, RemoteInputStream ris){
		super(server);
		_stub = stub;
		_ris = ris;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.gatherRanks(_ris);
	}
}
