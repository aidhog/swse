package org.semanticweb.swse.ann.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteSortByContextThread extends RMIThread<RemoteInputStream> {
	private RMIAnnRankingInterface _stub;
	
	public RemoteSortByContextThread(RMIAnnRankingInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.sortByContext();
	}
}
