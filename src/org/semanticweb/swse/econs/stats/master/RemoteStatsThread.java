package org.semanticweb.swse.econs.stats.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.stats.RMIEconsStatsInterface;
import org.semanticweb.swse.econs.stats.RMIEconsStatsServer.StatsResults;
import org.semanticweb.yars.nx.Node;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteStatsThread extends RMIThread<StatsResults> {
	private RMIEconsStatsInterface _stub;
	private String _spoc;
	private String _opsc;
	private Set<Node> _ignorePreds;
	
	public RemoteStatsThread(RMIEconsStatsInterface stub, int server, String spoc, String opsc, Set<Node> ignorePreds){
		super(server);
		_stub = stub;
		_ignorePreds = ignorePreds;
		_spoc = spoc;
		_opsc = opsc;
	}
	
	protected StatsResults runRemoteMethod() throws RemoteException{
		return _stub.stats(_spoc, _opsc, _ignorePreds);
	}
}
