package org.semanticweb.swse.lucene.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.lucene.RMILuceneInterface;
import org.semanticweb.swse.lucene.SlaveLuceneArgs;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteLuceneInitThread extends VoidRMIThread {
	private int _server;
	private RMILuceneInterface _stub;
	private RMIRegistries _servers;
	private SlaveLuceneArgs _sla;

	public RemoteLuceneInitThread(RMILuceneInterface stub, int server, RMIRegistries servers, SlaveLuceneArgs sla){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sla = sla;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sla);
	}
}
