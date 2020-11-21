package org.semanticweb.swse.econs.incon.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.incon.RMIEconsInconInterface;
import org.semanticweb.swse.econs.incon.SlaveEconsInconArgs;

/**
 * Thread to run init method on a remote server
 * @author aidhog
 *
 */
public class RemoteInconInitThread extends VoidRMIThread {
	private int _server;
	private RMIEconsInconInterface _stub;
	private RMIRegistries _servers;
	private SlaveEconsInconArgs _sra;
	private String _stubName;
	
	public RemoteInconInitThread(RMIEconsInconInterface stub, int server, RMIRegistries servers, SlaveEconsInconArgs sra, String stubName){
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
