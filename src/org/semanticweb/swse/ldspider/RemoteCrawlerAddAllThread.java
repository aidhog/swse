package org.semanticweb.swse.ldspider;

import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;


/**
 * Thread to run scatter method on a remote crawler (target 
 * crawler will distribute URIs from last round to other 
 * remote crawlers using hash method)
 * 
 * @author aidhog
 */
public class RemoteCrawlerAddAllThread extends VoidRMIThread {
	private RMICrawlerInterface _stub;
	private Map<String,Integer> _uris;
	private PldManager _pldm;
	private int _fromServer;

	public RemoteCrawlerAddAllThread(RMICrawlerInterface stub, int server, Map<String,Integer> uris, PldManager pldm, int fromServer){
		super(server);
		_stub = stub;
		_uris = uris;
		_pldm = pldm;
		_fromServer = fromServer;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.addAll(_uris, _pldm, _fromServer);
	}
}
