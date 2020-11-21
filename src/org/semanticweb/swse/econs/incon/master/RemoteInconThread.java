package org.semanticweb.swse.econs.incon.master;

import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.econs.incon.RMIEconsInconInterface;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to find inconsistencies on a remote server caused by consolidation
 * @author aidhog
 *
 */
public class RemoteInconThread extends RMIThread<Map<Node,Map<Node,SameAsIndex.SameAsList>>> {
	private RMIEconsInconInterface _stub;
	
	public RemoteInconThread(RMIEconsInconInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected Map<Node,Map<Node,SameAsIndex.SameAsList>> runRemoteMethod() throws RemoteException{
		return _stub.findInconsistencies();
	}
}
