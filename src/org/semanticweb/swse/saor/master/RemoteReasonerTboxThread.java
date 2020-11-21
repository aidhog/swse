package org.semanticweb.swse.saor.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.saor.RMIReasonerInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteReasonerTboxThread extends RMIThread<RemoteInputStream> {
	private RMIReasonerInterface _stub;

	public RemoteReasonerTboxThread(RMIReasonerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractTbox();
	}
}
