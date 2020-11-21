package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteSimpleQueryHitsThread extends RMIThread<ScoreDoc[]> {
	private RMIQueryInterface _stub;
	private String _keywordQ;
	private int _from, _to;
	private String _lang;

	public RemoteSimpleQueryHitsThread(RMIQueryInterface stub, int server, String keywordQ, int from, int to, String lang){
		super(server);
		_stub = stub;
		_keywordQ = keywordQ;
		_from = from;
		_to= to;
		_lang = lang;
	}
	
	protected ScoreDoc[] runRemoteMethod() throws RemoteException{
		return _stub.getKeywordHits(_keywordQ, _from, _to, _lang);
	}
}
