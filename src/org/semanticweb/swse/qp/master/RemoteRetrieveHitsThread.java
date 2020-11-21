package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * Thread to retrieve keyword hits from a remote server
 * @author aidhog
 *
 */
public class RemoteRetrieveHitsThread extends RMIThread<RemoteIterator<Node[]>> {
	private RMIQueryInterface _stub;
	private ScoreDoc[] _hits;
	private String _lang;

	public RemoteRetrieveHitsThread(RMIQueryInterface stub, int server, ScoreDoc[] hits, String lang){
		super(server);
		_stub = stub;
		_hits = hits;
		_lang = lang;
	}
	
	protected RemoteIterator<Node[]> runRemoteMethod() throws RemoteException{
		return _stub.getSnippets(_hits, _lang);
	}
}
