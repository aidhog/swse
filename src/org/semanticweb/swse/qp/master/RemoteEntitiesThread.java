package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * Thread to retrieve label/comment/whatever for an entity
 * @author aidhog
 *
 */
public class RemoteEntitiesThread extends RMIThread<RemoteIterator<Node[]>> {
	private RMIQueryInterface _stub;
	private Collection<Node> _entities;
	private String _lang;

	public RemoteEntitiesThread(RMIQueryInterface stub, int server, Collection<Node> entities, String prefLang){
		super(server);
		_stub = stub;
		_entities = entities;
	}
	
	protected RemoteIterator<Node[]> runRemoteMethod() throws RemoteException{
		return _stub.getEntities(_entities, _lang);
	}
}
