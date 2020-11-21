package org.semanticweb.swse.ann.incon;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.yars.stats.Count;

import com.healthmarketscience.rmiio.RemoteInputStream;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIAnnInconInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveAnnInconArgs sra) throws RemoteException;
	
	public Count<Statement> getCardinalities(Collection<Statement> patterns) throws RemoteException;
	
	public RemoteInputStream extractStatements(Collection<Statement> extract) throws RemoteException;
	
	public void reasonInconsistencies(LinkedRuleIndex<AnnotationRule<RankAnnotation>> aboxTemplateRuleIndex) throws RemoteException;
}
