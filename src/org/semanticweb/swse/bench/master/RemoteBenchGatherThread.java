package org.semanticweb.swse.bench.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.bench.RMIBenchInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteBenchGatherThread extends VoidRMIThread {
	private RMIBenchInterface _stub;
	private RemoteInputStream _ris;

	public RemoteBenchGatherThread(RMIBenchInterface stub, int server, RemoteInputStream ris){
		super(server);
		_stub = stub;
		_ris = ris;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.gather(_ris);
	}
}
