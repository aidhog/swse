package org.semanticweb.swse.rank.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.rank.RMIRankingInterface;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteRankNameAuthThread extends RMIThread<Set<Node[]>> {
	private RMIRankingInterface _stub;

	public RemoteRankNameAuthThread(RMIRankingInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected Set<Node[]> runRemoteMethod() throws RemoteException{
		return _stub.extractNamingAuthority();
	}
}
