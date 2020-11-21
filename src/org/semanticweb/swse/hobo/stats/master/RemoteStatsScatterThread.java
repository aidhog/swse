package org.semanticweb.swse.hobo.stats.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsInterface;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteStatsScatterThread extends VoidRMIThread {
	private RMIHoboStatsInterface _stub;
	private int[] _order;
	private Set<Node> _ignorePreds;
	
	public RemoteStatsScatterThread(RMIHoboStatsInterface stub, int server, int[] order, Set<Node> ignorePreds){
		super(server);
		_order = order;
		_ignorePreds = ignorePreds;
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatterTriples(_order, _ignorePreds);
	}
}
