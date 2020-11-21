package org.semanticweb.swse.index2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.index2.RMIIndexerInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteIndexerGetOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIIndexerInterface _stub;
	private int _server;

	public RemoteIndexerGetOutputStreamThread(RMIIndexerInterface stub, int server){
		super(server);
		_stub = stub;
		_server = server;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException {
		return _stub.getRemoteOutputStream(_server);
	}
}
