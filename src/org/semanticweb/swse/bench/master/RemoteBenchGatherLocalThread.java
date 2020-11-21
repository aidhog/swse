package org.semanticweb.swse.bench.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.bench.RMIBenchInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteBenchGatherLocalThread extends RMIThread<RemoteInputStream> {
	private RMIBenchInterface _stub;
	private String _infile;
	private boolean _gzip;

	public RemoteBenchGatherLocalThread(RMIBenchInterface stub, int server, String infile, boolean gzip){
		super(server);
		_stub = stub;
		_infile = infile;
		_gzip = gzip;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.gatherLocal(_infile, _gzip);
	}
}
