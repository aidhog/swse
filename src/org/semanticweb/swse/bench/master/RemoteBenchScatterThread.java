package org.semanticweb.swse.bench.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.bench.RMIBenchInterface;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteBenchScatterThread extends VoidRMIThread {
	private RMIBenchInterface _stub;
	private String _infile;
	private boolean _gzip;

	public RemoteBenchScatterThread(RMIBenchInterface stub, int server, String infile, boolean gzip){
		super(server);
		_stub = stub;
		_infile = infile;
		_gzip = gzip;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.scatter(_infile, _gzip);
	}
}
