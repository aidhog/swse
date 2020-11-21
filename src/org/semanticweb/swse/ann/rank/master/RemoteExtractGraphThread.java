package org.semanticweb.swse.ann.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to extract on-disk graph from remote server
 * @author aidhog
 *
 */
public class RemoteExtractGraphThread extends RMIThread<RemoteInputStream[]> {
	private RMIAnnRankingInterface _stub;
	private String _allContexts;
	
	public RemoteExtractGraphThread(RMIAnnRankingInterface stub, int server, String allContexts){
		super(server);
		_stub = stub;
		_allContexts = allContexts;
	}
	
	protected RemoteInputStream[] runRemoteMethod() throws RemoteException{
		return _stub.extractGraph(_allContexts);
	}
}
