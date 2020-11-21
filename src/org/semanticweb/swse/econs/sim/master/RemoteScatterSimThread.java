package org.semanticweb.swse.econs.sim.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteScatterSimThread extends VoidRMIThread {
	private RMIEconsSimInterface _stub;
	
	public RemoteScatterSimThread(RMIEconsSimInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatterSimilarity();
	}
}
