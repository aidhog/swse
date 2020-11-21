package org.semanticweb.swse.econs.ercons;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIConsolidationInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveConsolidationArgs sca, String stubName) throws RemoteException;
	
	public RemoteInputStream extractTbox() throws RemoteException;
	
	public RemoteInputStream extractStatements(Collection<Statement> extract) throws RemoteException;
	
	public RemoteInputStream extractStatements(Collection<Statement> extract, String tbox, Rule[] rules) throws RemoteException;
	
	public void consolidate(String sameAs) throws RemoteException;
	
	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException;
	
	public void sort() throws RemoteException;
}
