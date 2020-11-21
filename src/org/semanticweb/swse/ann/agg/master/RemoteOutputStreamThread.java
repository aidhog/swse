package org.semanticweb.swse.ann.agg.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.agg.RMIAnnAggregateInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to get output stream on a remote server
 * @author aidhog
 *
 */
public class RemoteOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIAnnAggregateInterface _stub;
	private String _filename; 
	private boolean _toGather;
	
	public RemoteOutputStreamThread(RMIAnnAggregateInterface stub, int server, String filename, boolean toGather){
		super(server);
		_stub = stub;
		_filename = filename;
		_toGather = toGather;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException{
		return _stub.getRemoteOutputStream(_filename, _toGather);
	}
}
