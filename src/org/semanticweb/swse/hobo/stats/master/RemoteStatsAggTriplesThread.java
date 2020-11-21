package org.semanticweb.swse.hobo.stats.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteStatsAggTriplesThread extends VoidRMIThread {
	private RMIHoboStatsInterface _stub;
	
	private int[] _order;
	private String _file;
	
	public RemoteStatsAggTriplesThread(RMIHoboStatsInterface stub, int server, int[] order, String file){
		super(server);
		_stub = stub;
		_order = order;
		_file = file;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.aggregateTriples(_order, _file);
	}
}
