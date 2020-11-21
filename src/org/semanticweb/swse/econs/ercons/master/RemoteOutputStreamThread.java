package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to get output stream on a remote server
 * @author aidhog
 *
 */
public class RemoteOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIConsolidationInterface _stub;
	private String _filename; 
	
	public RemoteOutputStreamThread(RMIConsolidationInterface stub, int server, String filename){
		super(server);
		_stub = stub;
		_filename = filename;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException{
		return _stub.getRemoteOutputStream(_filename);
	}
}
