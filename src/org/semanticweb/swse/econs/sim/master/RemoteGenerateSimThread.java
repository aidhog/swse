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
public class RemoteGenerateSimThread extends RMIThread<ArrayList<Integer>> {
	private RMIEconsSimInterface _stub;
	private ArrayList<HashMap<Node,PredStats>> _preds;
	
	public RemoteGenerateSimThread(RMIEconsSimInterface stub, int server, ArrayList<HashMap<Node,PredStats>> preds){
		super(server);
		_stub = stub;
		_preds = preds;
	}
	
	protected ArrayList<Integer> runRemoteMethod() throws RemoteException{
		return _stub.generateSimilarity(_preds);
	}
}
