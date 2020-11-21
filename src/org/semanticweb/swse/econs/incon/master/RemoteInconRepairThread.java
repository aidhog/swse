package org.semanticweb.swse.econs.incon.master;

import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.econs.incon.RMIEconsInconInterface;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to run repair method on a remote server
 * @author aidhog
 *
 */
public class RemoteInconRepairThread extends VoidRMIThread {
	private RMIEconsInconInterface _stub;
	private Map<Node,Map<Node,SameAsIndex.SameAsList>> _repair;
	
	public RemoteInconRepairThread(RMIEconsInconInterface stub, int server, Map<Node,Map<Node,SameAsIndex.SameAsList>> repair){
		super(server);
		_stub = stub;
		_repair = repair;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.repairInconsistencies(_repair);
	}
}
