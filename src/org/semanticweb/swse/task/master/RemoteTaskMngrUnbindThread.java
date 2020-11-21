package org.semanticweb.swse.task.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.task.RMITaskMngrInterface;

/**
 * Thread to run mkdirs method on a remote builder server
 * @author aidhog
 *
 */
public class RemoteTaskMngrUnbindThread extends VoidRMIThread {
	private RMITaskMngrInterface _stub;
	private String _hostname;
	private int _port;
	private String _name;

	public RemoteTaskMngrUnbindThread(RMITaskMngrInterface stub, int server, String hostname, int port, String name){
		super(server);
		_stub = stub;
		_name = name;
		_port = port;
		_hostname = hostname;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.unbind(_hostname, _port, _name);
	}
}
