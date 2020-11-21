package org.semanticweb.swse.hobo.stats.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsInterface;

import com.healthmarketscience.rmiio.RemoteOutputStream;

/**
 * Thread to get output stream on a remote server
 * @author aidhog
 *
 */
public class RemoteOutputStreamThread extends RMIThread<RemoteOutputStream> {
	private RMIHoboStatsInterface _stub;
	private String _filename; 
	
	public RemoteOutputStreamThread(RMIHoboStatsInterface stub, int server, String filename){
		super(server);
		_stub = stub;
		_filename = filename;
	}
	
	protected RemoteOutputStream runRemoteMethod() throws RemoteException{
		return _stub.getRemoteOutputStream(_filename);
	}
}
