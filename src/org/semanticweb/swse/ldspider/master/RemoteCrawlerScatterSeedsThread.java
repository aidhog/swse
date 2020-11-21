package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;


/**
 * Thread to scatter initial seeds to a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerScatterSeedsThread extends VoidRMIThread {
	private RMICrawlerInterface _stub;
	private Collection<String> _seeds;

	public RemoteCrawlerScatterSeedsThread(RMICrawlerInterface stub, int server, Collection<String> seeds){
		super(server);
		_stub = stub;
		_seeds = seeds;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.addSeeds(_seeds);
	}
}
