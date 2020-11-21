package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteSimpleQueryThread extends RMIThread<RemoteIterator<Node[]>> {
	private RMIQueryInterface _stub;
	private String _keywordQ;
	private int _from, _to;
	private String _lang;

	public RemoteSimpleQueryThread(RMIQueryInterface stub, int server, String keywordQ, int from, int to, String lang){
		super(server);
		_stub = stub;
		_keywordQ = keywordQ;
		_from = from;
		_to= to;
		_lang = lang;
	}
	
	protected RemoteIterator<Node[]> runRemoteMethod() throws RemoteException{
		return _stub.simpleQuery(_keywordQ, _from, _to, _lang);
	}
}
