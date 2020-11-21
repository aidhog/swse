package org.semanticweb.swse.rank.master;

import java.rmi.RemoteException;

import org.deri.idrank.pagerank.PageRankInfo;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.rank.RMIRankingInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteRankIDThread extends RMIThread<RemoteInputStream> {
	private RMIRankingInterface _stub;
	private PageRankInfo _naranks;

	public RemoteRankIDThread(RMIRankingInterface stub, int server, PageRankInfo naranks){
		super(server);
		_stub = stub;
		_naranks = naranks;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.getIdRank(_naranks);
	}
}
