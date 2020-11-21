package org.semanticweb.swse.index.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index.RMIIndexerInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteIndexerGatherThread extends VoidRMIThread {
	private RMIIndexerInterface _stub;
	private RemoteInputStream _ris;

	public RemoteIndexerGatherThread(RMIIndexerInterface stub, int server, RemoteInputStream ris){
		super(server);
		_stub = stub;
		_ris = ris;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.gather(_ris);
	}
}
