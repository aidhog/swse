package org.semanticweb.swse.ann.reason;

import java.rmi.RemoteException;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIAnnReasonerInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveAnnReasonerArgs sra) throws RemoteException;
	
	public RemoteInputStream extractTbox() throws RemoteException;
	
	public void reason(LinkedRuleIndex<AnnotationRule<RankAnnotation>> aboxTemplateRuleIndex) throws RemoteException;
}
