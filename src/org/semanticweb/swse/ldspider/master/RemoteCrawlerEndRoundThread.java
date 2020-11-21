package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;

/**
 * Thread to run endRound method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerEndRoundThread extends VoidRMIThread {
	private RMICrawlerInterface _stub;
	private boolean _join;

	public RemoteCrawlerEndRoundThread(RMICrawlerInterface stub, int server, boolean join){
		super(server);
		_stub = stub;
		_join = join;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.endRound(_join);
	}
}
