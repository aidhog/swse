package org.semanticweb.swse.bench.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.bench.RMIBenchInterface;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteBenchCloseThread extends VoidRMIThread {
	private RMIBenchInterface _stub;

	public RemoteBenchCloseThread(RMIBenchInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.close();
	}
}
