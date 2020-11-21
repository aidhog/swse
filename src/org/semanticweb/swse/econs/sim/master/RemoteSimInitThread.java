package org.semanticweb.swse.econs.sim.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;
import org.semanticweb.swse.econs.sim.SlaveEconsSimArgs;

/**
 * Thread to run init method on a remote server
 * @author aidhog
 *
 */
public class RemoteSimInitThread extends VoidRMIThread {
	private int _server;
	private RMIEconsSimInterface _stub;
	private RMIRegistries _servers;
	private SlaveEconsSimArgs _ssa;
	private String _stubName;
	
	public RemoteSimInitThread(RMIEconsSimInterface stub, int server, RMIRegistries servers, SlaveEconsSimArgs sra, String stubName){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_ssa = sra;
		_stubName = stubName;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _ssa, _stubName);
	}
}
