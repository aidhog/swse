package org.semanticweb.swse.econs.incon;

import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.yars.nx.Node;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIEconsInconInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveEconsInconArgs saa, String stubName) throws RemoteException;
	
	public Map<Node,Map<Node,SameAsList>> findInconsistencies() throws RemoteException;
	
	public void repairInconsistencies(Map<Node,Map<Node,SameAsList>> repair) throws RemoteException; 
}
