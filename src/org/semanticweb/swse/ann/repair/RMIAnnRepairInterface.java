package org.semanticweb.swse.ann.repair;

import java.rmi.RemoteException;
import java.util.ArrayList;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.index.StatementAnnotationMap;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;


/**
 * The RMI interface (stub) for RMI controllable consolidation
 * 
 * @author aidhog
 */
public interface RMIAnnRepairInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveAnnRepairArgs sra) throws RemoteException;
	
	public StatementAnnotationMap<RankAnnotation> getDeltaPlus(StatementAnnotationMap<RankAnnotation> delta) throws RemoteException;
	
	public ArrayList<StatementAnnotationMap<RankAnnotation>> getDeltaDMinus(StatementAnnotationMap<RankAnnotation> delta, StatementAnnotationMap<RankAnnotation> deltaPlus) throws RemoteException;
	
	public double[] repair(StatementAnnotationMap<RankAnnotation> deltaAll, StatementAnnotationMap<RankAnnotation> deltaD) throws RemoteException;
}
