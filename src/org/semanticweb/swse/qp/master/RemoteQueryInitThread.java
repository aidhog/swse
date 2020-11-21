package org.semanticweb.swse.qp.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.qp.RMIQueryInterface;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteQueryInitThread extends VoidRMIThread {
	private int _server;
	private RMIQueryInterface _stub;
	private RMIRegistries _servers;
	private String _sparse;
	private String _lucene;
	private String _spoc;

	public RemoteQueryInitThread(RMIQueryInterface stub, int server, RMIRegistries servers, String lucene, String spoc, String sparse){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sparse = sparse;
		_lucene = lucene;
		_spoc = spoc;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _lucene, _spoc, _sparse);
	}
}
