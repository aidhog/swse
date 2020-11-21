package org.semanticweb.swse.ann.agg;
//import java.rmi.RemoteException;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.agg.utils.AggregateTripleMaxRanksIterator;
import org.semanticweb.swse.ann.agg.utils.RemoteScatter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;


/**
 * Takes calls from the stub and translates into ranking actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering data.
 * 
 * @author aidhog
 */
public class RMIAnnAggregateServer implements RMIAnnAggregateInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3753950315603407560L;
	
	private final static Logger _log = Logger.getLogger(RMIAnnAggregateServer.class.getSimpleName());
	public final static String ID_RANK_FILENAME_UNSORTED = "idrank.nx.gz";
	public final static String ID_RANK_FILENAME_SORTED = "idrank.s.nx.gz";
	public final static String TEMP_DIR = "tmp";
	
	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	
	private transient SlaveAnnAggregateArgs _saa;
	
	public final static int TICKS = 10000000;
	
	private transient Vector<String> _toGatherFn;
	
	private transient RMIClient<RMIAnnAggregateInterface> _rmic;
	
	private final static NodeComparator NO_DUPES = new NodeComparator(true, true);

	public RMIAnnAggregateServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveAnnAggregateArgs saa, String stubName) throws RemoteException {
		long b4 = System.currentTimeMillis();
		try {
			RMIUtils.setLogFile(saa.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+saa);

		_saa = saa;
		
		_toGatherFn = new Vector<String>();
		
		RMIUtils.mkdirs(_saa.getOutDir());
		RMIUtils.mkdirs(_saa.getOutScatterDir());
		RMIUtils.mkdirs(_saa.getTmpDir());
		RMIUtils.mkdirsForFile(_saa.getOutFinal());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		_log.log(Level.INFO, "...connecting to peers...");
		
		try {
			_rmic = new RMIClient<RMIAnnAggregateInterface>(_servers, this, stubName);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_log.log(Level.INFO, "Connected.");

		_log.log(Level.INFO, "Connected in "+(System.currentTimeMillis() - b4)+" ms.");
	}
	
	public void clear() throws RemoteException {
		;
	}

	public RemoteOutputStream getRemoteOutputStream(String filename, boolean toGather) throws RemoteException {
		_log.info("Opening remote outputstream to file "+filename+"...");
		filename = RMIUtils.getLocalName(filename);
		RMIUtils.mkdirsForFile(filename);
		
		long b4 = System.currentTimeMillis();
		
		RemoteOutputStreamServer ostream = null;
		try{
			
			ostream = new SimpleRemoteOutputStream(new BufferedOutputStream(
				      new FileOutputStream(filename)));
			
			synchronized(this){
				if(toGather){
					_toGatherFn.add(filename);
				}
			}

			_log.info("...outputstream to file "+filename+" opened in "+(System.currentTimeMillis()-b4)+" ms.");
			return ostream;		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output stream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output stream on server "+_serverID+"\n"+e);
		}
	}
	
	public void scatterTriples() throws RemoteException {
		_log.info("Sorting and scattering triples...");
		String data = _saa.getIn();
		
		long b4 = System.currentTimeMillis();
		
		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(data);
			is = new GZIPInputStream(is); 
			
			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+data+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+data+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+data);
		
		SortArgs sa = new SortArgs(input);
		sa.setComparator(new NodeComparator(true, true));
		sa.setTicks(TICKS);
		sa.setTmpDir(_saa.getTmpDir());
		
		try{
			SortIterator si = new SortIterator(sa);
			
			RMIUtils.mkdirs(_saa.getOutScatterDir());
			RemoteScatter.scatter(si, _rmic, _saa.getOutScatterDir(), _saa.getRemoteGatherDir());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
		}
		
		_log.info("...data sorted and scattered in "+(System.currentTimeMillis()-b4)+" ms.");
	}
	
	public void aggregateRanks() throws RemoteException {
		_log.info("Aggregating ranked triples...");
		long b4 = System.currentTimeMillis();
		
		InputStream[] iss = new InputStream[_toGatherFn.size()+1];
		NxParser[] nxps = new NxParser[_toGatherFn.size()+1];
		
		try{
			iss[_toGatherFn.size()] = new FileInputStream(_saa.getRaw());
			if(_saa.getGzRaw())
				iss[_toGatherFn.size()] = new GZIPInputStream(iss[_toGatherFn.size()]);
			
			nxps[_toGatherFn.size()] = new NxParser(iss[_toGatherFn.size()]);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_saa.getRaw()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_saa.getRaw()+" on server "+_serverID+"\n"+e);
		}
		
		for(int i=0; i<_toGatherFn.size(); i++){
			try{
				_log.info("...opening "+_toGatherFn.get(i)+"...");
				iss[i] = new FileInputStream(_toGatherFn.get(i));
				iss[i] = new GZIPInputStream(iss[i]);
				
				nxps[i] = new NxParser(iss[i]);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error opening input file "+_toGatherFn.get(i)+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error opening input file "+_toGatherFn.get(i)+" on server "+_serverID+"\n"+e);
			}
		}
		
		_log.info("...output to "+_saa.getOutFinal()+"...");
		OutputStream os = null;
		try{
			os = new FileOutputStream(_saa.getOutFinal());
			os = new GZIPOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+_saa.getOutFinal()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+_saa.getOutFinal()+" on server "+_serverID+"\n"+e);
		}
		
		_log.info("...starting merge sort...");
		MergeSortArgs msa = new MergeSortArgs(nxps);
		msa.setComparator(NO_DUPES);
		msa.setTicks(TICKS);
		
		MergeSortIterator msi = new MergeSortIterator(msa);
		AggregateTripleMaxRanksIterator atri = new AggregateTripleMaxRanksIterator(msi);
		
		Callback cb = new CallbackNxOutputStream(os);
		
		int c = 0;
		while(atri.hasNext()){
			c++;
			if(TICKS>0 && c%TICKS==0){
				System.err.println("Processed "+c);
			}
			cb.processStatement(atri.next());
		}
		
		_toGatherFn.clear();
		
		atri.logStats();
		
		try{
			os.close();
			for(InputStream is:iss)
				is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing streams on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing streams on server "+_serverID+"\n"+e);
		}
		
		_log.info("...output "+c+" aggregated ranked triples.");
		_log.info("...triples aggregated in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIAnnAggregateServer rmi = new RMIAnnAggregateServer();
    	
    	Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}
}
