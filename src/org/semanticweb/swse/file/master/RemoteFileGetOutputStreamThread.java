package org.semanticweb.swse.file.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.file.RMIFileInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteFileGetOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIFileInterface _stub;
	private String _file;

	public RemoteFileGetOutputStreamThread(RMIFileInterface stub, int server, String file){
		super(server);
		_stub = stub;
		_file = file;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException {
		return _stub.getRemoteOutputStream(_file);
	}
}
