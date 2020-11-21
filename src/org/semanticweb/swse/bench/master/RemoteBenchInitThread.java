package org.semanticweb.swse.bench.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.bench.RMIBenchInterface;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteBenchInitThread extends VoidRMIThread {
	private int _server;
	private RMIBenchInterface _stub;
	private RMIRegistries _servers;
	private String _outdir;

	public RemoteBenchInitThread(RMIBenchInterface stub, int server, RMIRegistries servers, String outdir){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_outdir = outdir;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _outdir);
	}
}
