package org.semanticweb.swse.hobo.stats.master;

import java.rmi.RemoteException;
import java.util.Map;

import org.semanticweb.hobo.stats.PerPLDNamingStats.PerPLDStatsResults;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsInterface;

/**
 * Thread to rank triples on remote server
 * @author aidhog
 *
 */
public class RemoteStatsThread extends RMIThread<Map<String,PerPLDStatsResults>> {
	private RMIHoboStatsInterface _stub;
	private String _spoc;
	private String _opsc;
	
	public RemoteStatsThread(RMIHoboStatsInterface stub, int server, String spoc, String opsc){
		super(server);
		_stub = stub;
		_spoc = spoc;
		_opsc = opsc;
	}
	
	protected Map<String,PerPLDStatsResults> runRemoteMethod() throws RemoteException{
		return _stub.stats(_spoc, _opsc);
	}
}
