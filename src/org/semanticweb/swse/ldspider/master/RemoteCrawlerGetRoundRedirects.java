package org.semanticweb.swse.ldspider.master;

import java.net.URI;
import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;


/**
 * Thread to run runRound method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerGetRoundRedirects extends RMIThread<Map<URI,URI>> {
	private RMICrawlerInterface _stub;

	public RemoteCrawlerGetRoundRedirects(RMICrawlerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected Map<URI,URI> runRemoteMethod() throws RemoteException{
		return _stub.getRoundRedirects();
	}
}
