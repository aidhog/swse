package org.semanticweb.swse.econs.stats.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.stats.RMIEconsStatsInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to get output stream on a remote server
 * @author aidhog
 *
 */
public class RemoteOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIEconsStatsInterface _stub;
	private String _filename; 
	
	public RemoteOutputStreamThread(RMIEconsStatsInterface stub, int server, String filename){
		super(server);
		_stub = stub;
		_filename = filename;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException{
		return _stub.getRemoteOutputStream(_filename);
	}
}
