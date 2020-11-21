package org.semanticweb.swse.task.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.task.RMITaskMngrInterface;

/**
 * Thread to run mkdirs method on a remote builder server
 * @author aidhog
 *
 */
public class RemoteTaskMngrBindThread extends VoidRMIThread {
	private RMITaskMngrInterface _stub;
	private String _hostname;
	private int _port;
	private String _name;
	private RMIInterface _rmii;
	private boolean _startReg;

	public RemoteTaskMngrBindThread(RMITaskMngrInterface stub, int server, String hostname, int port, String name, RMIInterface rmii, boolean startReg){
		super(server);
		_stub = stub;
		_name = name;
		_port = port;
		_rmii = rmii;
		_hostname = hostname;
		_startReg = startReg;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.bind(_hostname, _port, _name, _rmii, _startReg);
	}
}
