package org.semanticweb.swse.rank;

import java.rmi.RemoteException;
import java.util.Set;

import org.deri.idrank.pagerank.PageRankInfo;
import org.semanticweb.swse.RMIInterface;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * The RMI interface (stub) for RMI controllable crawler
 * 
 * @author aidhog
 */
public interface RMIRankingInterface extends RMIInterface {
	public void init(int serverId, RMIRegistries servers, SlaveRankingArgs sra) throws RemoteException;
	
	public Set<Node[]> extractNamingAuthority() throws RemoteException;
	
	public RemoteInputStream getIdRank(PageRankInfo pri) throws RemoteException;
	
	public void gatherRanks(RemoteInputStream ranks) throws RemoteException;
}
