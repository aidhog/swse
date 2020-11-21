package org.semanticweb.swse.ldspider.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerSetup;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteCrawlerInitThread extends VoidRMIThread {
	private int _server;
	private RMICrawlerInterface _stub;
	private RemoteCrawlerSetup _rcs;
	private RMIRegistries _servers;

	public RemoteCrawlerInitThread(RMICrawlerInterface stub, int server, RMIRegistries servers, RemoteCrawlerSetup rcs){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_rcs = rcs;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _rcs);
	}
}
