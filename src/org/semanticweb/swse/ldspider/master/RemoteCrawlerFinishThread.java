package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;


/**
 * Thread to run finish method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerFinishThread extends VoidRMIThread {
	private RMICrawlerInterface _stub;

	public RemoteCrawlerFinishThread(RMICrawlerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.finish();
	}
}
