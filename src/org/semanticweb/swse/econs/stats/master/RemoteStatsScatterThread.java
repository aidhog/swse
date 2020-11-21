package org.semanticweb.swse.econs.stats.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.stats.RMIEconsStatsInterface;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteStatsScatterThread extends VoidRMIThread {
	private RMIEconsStatsInterface _stub;
	private Set<Node> _ignorePreds;
	private int[] _order;
	
	public RemoteStatsScatterThread(RMIEconsStatsInterface stub, int server, int[] order, Set<Node> ignorePreds){
		super(server);
		_order = order;
		_ignorePreds = ignorePreds;
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatterTriples(_order, _ignorePreds);
	}
}
