package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;


/**
 * Thread to run runRound method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerRunRoundThread extends RMIThread<Integer> {
	private RMICrawlerInterface _stub;
	private int _targeturis;

	public RemoteCrawlerRunRoundThread(RMICrawlerInterface stub, int server, int targeturis){
		super(server);
		_stub = stub;
		_targeturis = targeturis;
	}
	
	protected Integer runRemoteMethod() throws RemoteException{
		return _stub.runRound(_targeturis);
	}
}
