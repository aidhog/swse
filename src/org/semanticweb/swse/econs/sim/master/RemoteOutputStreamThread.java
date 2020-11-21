package org.semanticweb.swse.econs.sim.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to get output stream on a remote server
 * @author aidhog
 *
 */
public class RemoteOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIEconsSimInterface _stub;
	private String _filename; 
	
	public RemoteOutputStreamThread(RMIEconsSimInterface stub, int server, String filename){
		super(server);
		_stub = stub;
		_filename = filename;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException{
		return _stub.getRemoteOutputStream(_filename);
	}
}
