package org.semanticweb.swse.cons2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidationIFPsFPsThread extends RMIThread<RemoteInputStream> {
	private RMIConsolidationInterface _stub;
	private boolean _reason;

	public RemoteConsolidationIFPsFPsThread(RMIConsolidationInterface stub, int server, boolean reason){
		super(server);
		_stub = stub;
		_reason = reason;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.getIFPsandFPs(_reason);
	}
}
