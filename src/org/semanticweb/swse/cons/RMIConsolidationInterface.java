package org.semanticweb.swse.cons;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.cons.utils.SameAsIndex;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIConsolidationInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveConsolidationArgs sca) throws RemoteException;
	
	public RemoteInputStream extractSameAs() throws RemoteException;
	
	public int[] consolidate(SameAsIndex sai, int[] els) throws RemoteException;
}
