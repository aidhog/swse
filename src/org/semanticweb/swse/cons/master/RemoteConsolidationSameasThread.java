package org.semanticweb.swse.cons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.cons.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidationSameasThread extends RMIThread<RemoteInputStream> {
	private RMIConsolidationInterface _stub;

	public RemoteConsolidationSameasThread(RMIConsolidationInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractSameAs();
	}
}
