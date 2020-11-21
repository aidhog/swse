package org.semanticweb.swse.econs.sim.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;
import org.semanticweb.swse.econs.sim.utils.Stats;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteAggregateSimThread extends RMIThread<Stats<Double>> {
	private RMIEconsSimInterface _stub;
	
	public RemoteAggregateSimThread(RMIEconsSimInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected Stats<Double> runRemoteMethod() throws RemoteException{
		return _stub.aggregateSimilarity();
	}
}
