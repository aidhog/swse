package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidationExtractTboxThread extends RMIThread<RemoteInputStream> {
	private RMIConsolidationInterface _stub;

	public RemoteConsolidationExtractTboxThread(RMIConsolidationInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractTbox();
	}
}
