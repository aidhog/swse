package org.semanticweb.swse.file.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.file.RMIFileInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteSendFileThread extends VoidRMIThread {
	private RMIFileInterface _stub;
	private RemoteInputStream _ris;
	private String _filename;

	public RemoteSendFileThread(RMIFileInterface stub, int server, RemoteInputStream ris, String filename){
		super(server);
		_stub = stub;
		_ris = ris;
		_filename = filename;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.receiveFile(_ris, _filename);
	}
}
