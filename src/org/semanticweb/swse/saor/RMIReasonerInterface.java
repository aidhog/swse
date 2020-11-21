package org.semanticweb.swse.saor;

import java.rmi.RemoteException;

import org.semanticweb.saorr.index.StatementStore;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIReasonerInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveReasonerArgs sra) throws RemoteException;
	
	public RemoteInputStream extractTbox() throws RemoteException;
	
	@Deprecated
	public void reason(StatementStore tbox) throws RemoteException;
	
	public void reason(LinkedRuleIndex<Rule> aboxTemplateRuleIndex) throws RemoteException;
}
