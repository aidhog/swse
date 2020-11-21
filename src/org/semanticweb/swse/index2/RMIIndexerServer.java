package org.semanticweb.swse.index2;
//import java.rmi.RemoteException;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.nxindex.block.NodesBlockWriterIO;
import org.semanticweb.nxindex.sparse.SparseIndex;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.index2.utils.RemoteScatter;
import org.semanticweb.swse.index2.utils.RemoveReasonedDupesIterator;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;

import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIIndexerServer implements RMIIndexerInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2193027625923621023L;
	private final static Logger _log = Logger.getLogger(RMIIndexerServer.class.getSimpleName());
	public final static String GATHER_FILE = "gather.nq.gz";
	public final static String SCATTER_FILE = "scatter.nq.gz";

	public final static int BUFFER_SIZE = 1024;
	
	private transient int _fileCount = 0;
	
	private transient RMIClient<RMIIndexerInterface> _rmic;
	
	public final static int TICKS = 1000000;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	
	private transient Vector<String> _batches;

	private transient SlaveIndexerArgs _sia;
	
	private transient int _dupes = 0;

	private transient ArrayList<RemoteOutputStreamServer> _toClose = new ArrayList<RemoteOutputStreamServer>();



	public RMIIndexerServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveIndexerArgs sia, String stubName) throws RemoteException {
		try {
			RMIUtils.setLogFile(sia.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sia.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sia.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_sia = sia;
		RMIUtils.mkdirs(_sia.getOutGatherDir());
		RMIUtils.mkdirs(_sia.getOutScatterDir());
		RMIUtils.mkdirsForFile(_sia.getOutIndex());
		RMIUtils.mkdirsForFile(_sia.getOutSparse());
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sia);

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_batches = new Vector<String>();

		_log.log(Level.INFO, "...connecting to peers...");
		try {
			_rmic = new RMIClient<RMIIndexerInterface>(_servers, this, stubName);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public void makeIndex() throws RemoteException {
		_log.info("Making index file...");
		long b4 = System.currentTimeMillis();
		
		InputStream is[] = new InputStream[_batches.size()];
		Iterator<Node[]>[] in = new Iterator[_batches.size()];
		
		_log.info("...merging "+_batches.size()+" batches...");
		
		for(int i=0; i<in.length; i++){
			try {
				is[i] = new GZIPInputStream(new FileInputStream(_batches.get(i)));
				in[i] = new NxParser(is[i]);
			} catch (Exception e) {
				_log.log(Level.SEVERE, "Error opening batch file on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error opening batch file on server "+_serverID+"\n"+e);
			}

		}

		String outfile = _sia.getOutIndex();

		NodesBlockWriterIO nbo = null;
		try {
			nbo = new NodesBlockWriterIO(outfile, (short)4);
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error opening index file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening index file on server "+_serverID+"\n"+e);
		}


		_log.info("Merging "+_batches.size()+" batches.");
		MergeSortIterator msi = new MergeSortIterator(in);
		RemoveReasonedDupesIterator rrdi = new RemoveReasonedDupesIterator(msi);
		
		int c = 0;
		try{
			while(rrdi.hasNext()){
				c++;
				if(c%TICKS==0){
					_log.info("...merge sorted "+c+" statements...");
				}
				nbo.write(rrdi.next());
			}

			nbo.close();
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error writing index file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error writing index file on server "+_serverID+"\n"+e);
		}
		
		
		long dupes = msi.duplicates();
		_dupes+=dupes;
		_dupes+=rrdi.duplicatesRemoved();
		_log.info("MergeSortIterator found "+dupes+" duplicates...");
		_log.info("RemoveReasonedDupesIterator found "+rrdi.duplicatesRemoved()+" duplicates...");
		_log.info("Total duplicates "+_dupes+" statements...");
		_log.info("RemoveReasonedDupesIterator wrote "+c+" statements...");
		_log.info(c+" statements written to index...");
		
		try{
			NodesBlockReaderNIO nbr = new NodesBlockReaderNIO(outfile);
			SparseIndex si = new SparseIndex(nbr);

			String sparsefile = _sia.getOutSparse();
			si.store(sparsefile);
			
			nbr.close();
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error writing sparse file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error writing sparse file on server "+_serverID+"\n"+e);
		}
		
		_log.info("...index file and sparse built in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public RemoteOutputStream getRemoteOutputStream(int forServer) throws RemoteException {
		_log.info("Opening remote outputstream for server "+forServer+"...");
		
		String filename = _sia.getOutGatherDir()+"/"+(_fileCount++)+"."+forServer+"."+GATHER_FILE;
		
		long b4 = System.currentTimeMillis();
		
		RemoteOutputStreamServer ostream = null;
		try{
			
			ostream = new SimpleRemoteOutputStream(new BufferedOutputStream(
				      new FileOutputStream(filename)));
			
			_log.info("...outputstream to file "+filename+" opened in "+(System.currentTimeMillis()-b4)+" ms.");
			_toClose.add(ostream);
			_batches.add(filename);
			return ostream;		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output stream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output stream on server "+_serverID+"\n"+e);
		}
	}

	public void scatter(String[] infile, boolean[] gzip) throws RemoteException {
		long b4 = System.currentTimeMillis();
		_log.info("Scattering "+infile.length+" files...");
		try{
			RemoteScatter.scatter(infile, gzip, _rmic, _sia.getOutScatterDir(), SCATTER_FILE);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error scattering on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error scattering on server "+_serverID+"\n"+e);
		}
		_log.info("...scattering file done in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIIndexerServer rmi = new RMIIndexerServer();

		RMIIndexerInterface stub = (RMIIndexerInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		_sia = null;
		
		for(RemoteOutputStreamServer ros:_toClose)
			ros.close();
	}

}
