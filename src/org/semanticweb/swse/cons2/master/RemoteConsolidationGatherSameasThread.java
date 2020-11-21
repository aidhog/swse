package org.semanticweb.swse.cons2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationGatherSameasThread extends VoidRMIThread {
	private RMIConsolidationInterface _stub;
	private RemoteInputStream _ris;

	public RemoteConsolidationGatherSameasThread(RMIConsolidationInterface stub, int server, RemoteInputStream ris){
		super(server);
		_stub = stub;
		_ris = ris;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.gatherSameAs(_ris);
	}
}
