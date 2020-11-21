package org.semanticweb.swse.ann.reason.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteAnnReasonerTboxThread extends RMIThread<RemoteInputStream> {
	private RMIAnnReasonerInterface _stub;

	public RemoteAnnReasonerTboxThread(RMIAnnReasonerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractTbox();
	}
}
