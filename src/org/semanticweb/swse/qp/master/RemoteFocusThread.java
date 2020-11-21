package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * Thread to retrieve quads for an entity
 * @author aidhog
 *
 */
public class RemoteFocusThread extends RMIThread<RemoteIterator<Node[]>> {
	private RMIQueryInterface _stub;
	private Node _n;

	public RemoteFocusThread(RMIQueryInterface stub, int server, Node n){
		super(server);
		_stub = stub;
		_n = n;
	}
	
	protected RemoteIterator<Node[]> runRemoteMethod() throws RemoteException{
		return _stub.getFocus(_n);
	}
}
