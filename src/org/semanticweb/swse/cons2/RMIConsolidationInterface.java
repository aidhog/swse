package org.semanticweb.swse.cons2;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIConsolidationInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, String infile, boolean gzip, String outdir, String tmpdir) throws RemoteException;
	
	public RemoteInputStream getIFPsandFPs(boolean reason) throws RemoteException;
	
	public RemoteInputStream getConsolidationTriples(Set<Node> ifps, Set<Node> fps) throws RemoteException;
	
	public void sort() throws RemoteException;
	
	public void gatherSameAs(RemoteInputStream inFile) throws RemoteException;
	
	public void consolidate() throws RemoteException;
	
}
