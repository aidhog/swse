package org.semanticweb.swse.ann.rank.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.rank.RMIAnnRankingInterface;
import org.semanticweb.swse.ann.rank.SlaveAnnRankingArgs;

/**
 * Thread to run init method on a remote server
 * @author aidhog
 *
 */
public class RemoteRankInitThread extends VoidRMIThread {
	private int _server;
	private RMIAnnRankingInterface _stub;
	private RMIRegistries _servers;
	private SlaveAnnRankingArgs _sra;
	private String _stubName;
	
	public RemoteRankInitThread(RMIAnnRankingInterface stub, int server, RMIRegistries servers, SlaveAnnRankingArgs sra, String stubName){
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
