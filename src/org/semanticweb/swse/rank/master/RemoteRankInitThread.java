package org.semanticweb.swse.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.rank.RMIRankingInterface;
import org.semanticweb.swse.rank.SlaveRankingArgs;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteRankInitThread extends VoidRMIThread {
	private int _server;
	private RMIRankingInterface _stub;
	private RMIRegistries _servers;
	private SlaveRankingArgs _sra; 
	
	public RemoteRankInitThread(RMIRankingInterface stub, int server, RMIRegistries servers, SlaveRankingArgs sra){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_sra = sra;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _sra);
	}
}
