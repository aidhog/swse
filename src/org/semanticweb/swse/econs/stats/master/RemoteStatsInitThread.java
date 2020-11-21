package org.semanticweb.swse.econs.stats.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.stats.RMIEconsStatsInterface;
import org.semanticweb.swse.econs.stats.SlaveEconsStatsArgs;

/**
 * Thread to run init method on a remote server
 * @author aidhog
 *
 */
public class RemoteStatsInitThread extends VoidRMIThread {
	private int _server;
	private RMIEconsStatsInterface _stub;
	private RMIRegistries _servers;
	private SlaveEconsStatsArgs _sra;
	private String _stubName;
	
	public RemoteStatsInitThread(RMIEconsStatsInterface stub, int server, RMIRegistries servers, SlaveEconsStatsArgs sra, String stubName){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sra = sra;
		_stubName = stubName;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sra, _stubName);
	}
}
