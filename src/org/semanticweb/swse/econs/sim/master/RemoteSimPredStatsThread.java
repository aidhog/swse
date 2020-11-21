package org.semanticweb.swse.econs.sim.master;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteSimPredStatsThread extends RMIThread<ArrayList<HashMap<Node,PredStats>>> {
	private RMIEconsSimInterface _stub;
	
	public RemoteSimPredStatsThread(RMIEconsSimInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected ArrayList<HashMap<Node,PredStats>> runRemoteMethod() throws RemoteException{
		return _stub.predicateStatistics();
	}
}
