package org.semanticweb.swse.index2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index2.RMIIndexerInterface;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteIndexerScatterThread extends VoidRMIThread {
	private RMIIndexerInterface _stub;
	private String[] _infiles;
	private boolean[] _gzip;

	public RemoteIndexerScatterThread(RMIIndexerInterface stub, int server, String[] infiles, boolean[] gzip){
		super(server);
		_stub = stub;
		_infiles = infiles;
		_gzip = gzip;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatter(_infiles, _gzip);
	}
}
