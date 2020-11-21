package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;


/**
 * Thread to run scatter method on a remote crawler (target 
 * crawler will distribute URIs from last round to other 
 * remote crawlers using hash method)
 * 
 * @author aidhog
 */
public class RemoteCrawlerScatterThread extends VoidRMIThread {
	private RMICrawlerInterface _stub;

	public RemoteCrawlerScatterThread(RMICrawlerInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatter();
	}
}
