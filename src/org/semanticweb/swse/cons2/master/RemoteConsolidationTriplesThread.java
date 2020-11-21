package org.semanticweb.swse.cons2.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidationTriplesThread extends RMIThread<RemoteInputStream> {
	private RMIConsolidationInterface _stub;
	private Set<Node> _ifps;
	private Set<Node> _fps;

	public RemoteConsolidationTriplesThread(RMIConsolidationInterface stub, int server, Set<Node> ifps, Set<Node> fps){
		super(server);
		_stub = stub;
		_ifps = ifps;
		_fps = fps;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.getConsolidationTriples(_ifps, _fps);
	}
}
