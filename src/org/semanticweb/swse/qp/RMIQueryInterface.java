package org.semanticweb.swse.qp;

import java.rmi.RemoteException;
import java.util.Collection;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * The RMI interface (stub) for RMI controllable indexing
 * 
 * @author aidhog
 */
public interface RMIQueryInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, String lucene, String spoc, String sparse) throws RemoteException;
	
	public RemoteIterator<Node[]> simpleQuery(String keywordQ, int from, int to, String prefLangPrefix) throws RemoteException;
	
	public ScoreDoc[] getKeywordHits(String keywordQ, int from, int to, String prefLangPrefix) throws RemoteException;
	
	public RemoteIterator<Node[]> getSnippets(ScoreDoc[] hits, String prefLangPrefix) throws RemoteException;
	
	public RemoteIterator<Node[]> getFocus(Node n) throws RemoteException;
	
	public RemoteIterator<Node[]> getEntities(Collection<Node> n, String prefLangPrefix) throws RemoteException;
}
