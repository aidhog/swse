package org.semanticweb.swse.index;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.nxindex.Constants;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.nxindex.block.NodesBlockWriterIO;
import org.semanticweb.nxindex.sparse.SparseIndex;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index.master.RemoteIndexerGatherThread;
import org.semanticweb.swse.index.utils.CallbackSortedBatches;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;


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
	
	private transient RMIClient<RMIIndexerInterface> _rmic;
	
	public final static int TICKS = 1000000;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveIndexerArgs _sia;
	
	private transient int _dupes = 0;

	private transient CallbackSortedBatches _cb;

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

		_log.log(Level.INFO, "...connecting to peers...");
		try {
			_rmic = new RMIClient<RMIIndexerInterface>(_servers, this, stubName);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_cb = new CallbackSortedBatches(sia.getOutGatherDir(), Constants.estimateMaxStatements(4));

		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public void makeIndex() throws RemoteException {
		_log.info("Making index file...");
		long b4 = System.currentTimeMillis();
		
		ArrayList<String> gathered = null;
		try{
			gathered = _cb.getBatches();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening batch file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening batch file on server "+_serverID+"\n"+e);
		}
		
		
		InputStream is[] = new InputStream[gathered.size()];
		Iterator<Node[]>[] in = new Iterator[gathered.size()];
		for(int i=0; i<in.length; i++){
			try {
				is[i] = new GZIPInputStream(new FileInputStream(gathered.get(i)));
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


		_log.info("Merging "+gathered.size()+" batches of max size "+_cb.getLimit());
		MergeSortIterator msi = new MergeSortIterator(in);

		int c = 0;
		try{
			while(msi.hasNext()){
				c++;
				if(c%TICKS==0){
					_log.info("...merge sorted "+c+" statements...");
				}
				nbo.write(msi.next());
			}

			nbo.close();
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error writing index file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error writing index file on server "+_serverID+"\n"+e);
		}
		
		
		long dupes = msi.duplicates();
		_dupes+=dupes;
		_log.info("MergeSortIterator found "+dupes+" duplicates...");
		_log.info("Total duplicates "+_dupes+" statements...");
		_log.info("MergeSortIterator wrote "+c+" statements...");
		
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

	public void gather(RemoteInputStream inFile) throws RemoteException {
		_log.info("Gathering file...");
		long b4 = System.currentTimeMillis();
		InputStream is = null;
		int dupes = 0;
		TreeSet<Node[]> buffer = new TreeSet<Node[]>(NodeComparator.NC);
		try{
			is = RemoteInputStreamClient.wrap(inFile);
			is = new GZIPInputStream(is);

			NxParser nxp = new NxParser(is);

			while(nxp.hasNext()){
				if(!buffer.add(nxp.next()))
					dupes++;
				if(buffer.size()==BUFFER_SIZE){
					_cb.processStatements(buffer);
					buffer = new TreeSet<Node[]>(NodeComparator.NC);
				}
			}
			_cb.processStatements(buffer);
			buffer = null;
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error gathering file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error gathering file on server "+_serverID+"\n"+e);
		} 
		_log.info("...file gathered in "+(System.currentTimeMillis()-b4)+" ms. Found "+dupes+" duplicates.");
		_dupes+=dupes;
	}

	public void scatter(String[] infile, boolean[] gzip) throws RemoteException {
		ArrayList<VoidRMIThread> gatherThreads = new ArrayList<VoidRMIThread>();
		if(infile.length!=gzip.length){
			throw new RemoteException("Need a gzip entry for every infile arg!");
		}
		
		ArrayList<RemoteInputStreamServer> riss = new ArrayList<RemoteInputStreamServer>();
		
		for(int i=0; i<infile.length; i++){
			_log.info("Scattering "+infile[i]+" to remote servers...");
			String[] files = null;
			try{
				InputStream is = new FileInputStream(infile[i]);
				if(gzip[i]){
					is = new GZIPInputStream(is);
				}
				NxParser nxp = new NxParser(is);

				files = split(nxp, 0, i);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error splitting local file on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error splitting local file on server "+_serverID+"\n"+e);
			}
			for(int j=0; j<files.length; j++){
				RemoteInputStreamServer istream = null;

				riss.add(istream);
				
				try {
					istream = new SimpleRemoteInputStream(new BufferedInputStream(
							new FileInputStream(files[j])));
					RemoteInputStream result = istream.export();
					riss.add(istream);

					_log.info("Scattering "+files[j]+" to remote server");
					RMIIndexerInterface rmii = _rmic.getStub(j);

					RemoteIndexerGatherThread rigt = new RemoteIndexerGatherThread(rmii, j, result);

					rigt.start();

					gatherThreads.add(rigt);
				} catch(IOException e){ 
					_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
					e.printStackTrace();
					throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
				}
			}
		}

		_log.log(Level.INFO, "...awaiting threads return...");
		long b4 = System.currentTimeMillis();
		try{
			for(VoidRMIThread t:gatherThreads){
				_log.log(Level.INFO, "...awaiting return of thread "+t+"...");
				t.join();
			}
		} catch (Exception e){
			_log.log(Level.SEVERE, "Error waiting for gather thread "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error waiting for gather thread "+_serverID+"\n"+e);
		}
		
		for(RemoteInputStreamServer ris:riss){
			if(ris!=null) ris.close();
		}
		_log.info("...joining threads took "+(System.currentTimeMillis()-b4)+" ms.");
	}

	private String[] split(Iterator<Node[]> in, int el, int f) throws FileNotFoundException, IOException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = _servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		for(int i=0; i<files; i++){
			String outfile = _sia.getOutScatterDir()+"/"+f+"."+i+"."+SCATTER_FILE;
			fns[i] = outfile;
			os[i] = new GZIPOutputStream(new FileOutputStream(outfile));
			cb[i] = new CallbackNxOutputStream(os[i]);
		}

		int c = 0;
		while(in.hasNext()){
			c++;
			if(c%TICKS==0){
				_log.info("...split "+c+" statements...");
			}
			
			Node[] next = in.next();
			int server = _servers.getServerNo(next[el]);
			cb[server].processStatement(next);
		}

		for(int i=0; i<files; i++){
			os[i].close();
		}
		_log.info("...splitting file done in "+(System.currentTimeMillis()-b4)+" ms... split "+c+" statements.");
		return fns;
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
		_cb = null;
	}

}
