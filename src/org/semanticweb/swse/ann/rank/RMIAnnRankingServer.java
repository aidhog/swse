package org.semanticweb.swse.ann.rank;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
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

import org.deri.idrank.pagerank.OnDiskPageRank;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.utils.AggregateTripleRanksIterator;
import org.semanticweb.swse.ann.rank.utils.ExtractGraphIterator;
import org.semanticweb.swse.ann.rank.utils.RankTriplesIterator;
import org.semanticweb.swse.ann.rank.utils.RemoteScatter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;


/**
 * Takes calls from the stub and translates into ranking actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering data.
 * 
 * @author aidhog
 */
public class RMIAnnRankingServer implements RMIAnnRankingInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3753950315603407560L;
	
	private final static Logger _log = Logger.getLogger(RMIAnnRankingServer.class.getSimpleName());
	public final static String ID_RANK_FILENAME_UNSORTED = "idrank.nx.gz";
	public final static String ID_RANK_FILENAME_SORTED = "idrank.s.nx.gz";
	public final static String TEMP_DIR = "tmp";
	
	public final static int[] CONTEXT_SORT_ORDER = {3,0,1,2};

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	
	private transient SlaveAnnRankingArgs _sra;
	
	public final static int TICKS = 10000000;
	
	private transient Vector<String> _toGatherFn;
	
	private transient RMIClient<RMIAnnRankingInterface> _rmic;
	
	private final static NodeComparator NO_DUPES = new NodeComparator(true, true);

	public RMIAnnRankingServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveAnnRankingArgs sra, String stubName) throws RemoteException {
		long b4 = System.currentTimeMillis();
		try {
			RMIUtils.setLogFile(sra.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sra);

		_sra = sra;
		
		_toGatherFn = new Vector<String>();
		
		RMIUtils.mkdirs(_sra.getOutDir());
		RMIUtils.mkdirsForFile(_sra.getContexts());
		RMIUtils.mkdirsForFile(_sra.getInvGraphFragment());
		RMIUtils.mkdirsForFile(_sra.getRawInvGraphFragment());
		RMIUtils.mkdirsForFile(_sra.getRedirects());
		RMIUtils.mkdirsForFile(_sra.getSortedByContext());
		RMIUtils.mkdirsForFile(_sra.getTmpDir());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		_log.log(Level.INFO, "...connecting to peers...");
		
		try {
			_rmic = new RMIClient<RMIAnnRankingInterface>(_servers, this, stubName);
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
	
	public RemoteInputStream sortByContext() throws RemoteException {
		_log.info("Sorting raw data by context and extracting sorted contexts...");
		long b4 = System.currentTimeMillis();
		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(_sra.getIn());
			if(_sra.getGzIn()){
				is = new GZIPInputStream(is); 
			}
			
			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
		}
		_log.info("Sorting from "+_sra.getIn()+"...");
		
		OutputStream os = null;
		try{
			os = new FileOutputStream(_sra.getSortedByContext());
			os = new GZIPOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
		}
		_log.info("Sorting to "+_sra.getSortedByContext()+"...");
		
		OutputStream os2 = null;
		try{
			os2 = new FileOutputStream(_sra.getContexts());
			os2 = new GZIPOutputStream(os2);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+_sra.getContexts()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+_sra.getContexts()+" on server "+_serverID+"\n"+e);
		}
		_log.info("Extracting contexts to "+_sra.getContexts()+"...");
		
		Callback maincb = new CallbackNxOutputStream(os);
		Callback concb = new CallbackNxOutputStream(os2);
		
		ExtractContextsCallback cb = new ExtractContextsCallback(maincb, concb);
		
		NodeComparatorArgs nc_con_a = new NodeComparatorArgs();
		nc_con_a.setOrder(CONTEXT_SORT_ORDER);
		NodeComparator nc_con = new NodeComparator(nc_con_a);
		
		SortArgs sa = new SortArgs(input);
		sa.setTmpDir(_sra.getTmpDir());
		sa.setComparator(nc_con);
		sa.setTicks(TICKS);
		
		_log.info("Starting sort...");
		
		try{
			SortIterator si = new SortIterator(sa);
			int c = 0;
			while(si.hasNext()){
				cb.processStatement(si.next());
				c++;
				if(TICKS>0 && c%TICKS==0){
					_log.info("Merged "+c+" triples from input...");
				}
			}
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error iterating over input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error iterating over input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
		}
		
		try{
			is.close();
			os.close();
			os2.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing input/output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing input/output file on server "+_serverID+"\n"+e);
		}
		
		_log.info("Finished sorting/extracting contexts... extracted "+cb.getConsProcessed()+" contexts from "+cb.getAllProcessed()+" sorted triples in "+(System.currentTimeMillis()-b4)+" ms.");
		
		RemoteInputStreamServer istream = null;

		_log.info("Exporting stream for results...");
		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sra.getContexts())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			_log.info("...returning stream.");
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			if(istream != null) istream.close();
		}
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
	
	public RemoteInputStream[] extractGraph(String allContexts) throws RemoteException {
		_log.info("Extracting graph...");
		long b4 = System.currentTimeMillis();
		
		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(_sra.getSortedByContext());
			is = new GZIPInputStream(is); 
			
			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getIn()+" on server "+_serverID+"\n"+e);
		}
		
		_log.info("Input set to "+_sra.getSortedByContext()+"...");
		
		_log.info("Extracting graph, sorting inverted links, and rewriting according to redirects in "+_sra.getRedirects()+" ...");
		
		ExtractGraphIterator egi = new ExtractGraphIterator(input, _sra.getTbox());
		ReorderIterator ri = new ReorderIterator(egi, new int[]{1,0});
		
		SortArgs sa = new SortArgs(ri);
		sa.setTicks(TICKS);
		sa.setTmpDir(_sra.getTmpDir());
		
		try{
			SortIterator si = new SortIterator(sa);
			OnDiskPageRank.ResetableNxInput redirects = new OnDiskPageRank.ResetableNxInput(_sra.getRedirects(), _sra.getGzRedirects());
			
			OnDiskPageRank.rewrite(si, redirects, _sra.getRawInvGraphFragment());
			
			redirects.close();
			is.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error extracting/rewriting graph from input file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error extracting/rewriting graph from input file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
		}
		
		_log.info("...initial graph sorted and rewritten to "+_sra.getRawInvGraphFragment());
		
		_log.info("Pruning graph at "+_sra.getRawInvGraphFragment());
		try{
			is = new FileInputStream(_sra.getRawInvGraphFragment());
			is = new GZIPInputStream(is); 
			
			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getRawInvGraphFragment()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getRawInvGraphFragment()+" on server "+_serverID+"\n"+e);
		}


		_log.info("...keeping vertices in "+allContexts+"...");
		InputStream is2 = null;
		NxParser contexts = null;
		try{
			is2 = new FileInputStream(allContexts);
			is2 = new GZIPInputStream(is2); 
			
			contexts = new NxParser(is2);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+allContexts+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+allContexts+" on server "+_serverID+"\n"+e);
		}
		
		try{
			OnDiskPageRank.prune(input, contexts, _sra.getInvGraphFragment());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error pruning graph "+_sra.getRawInvGraphFragment()+" with contexts "+allContexts+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error pruning graph "+_sra.getRawInvGraphFragment()+" with contexts "+allContexts+" on server "+_serverID+"\n"+e);
		}
		
		try{
			is.close();
			is2.close(); 
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing streams on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing streams on server "+_serverID+"\n"+e);
		}
		
		_log.info("Finished preparing inverted graph fragment in "+(System.currentTimeMillis()-b4)+"ms.");
		
		_log.info("Sorting natural order graph...");
		_log.info("...sorting from "+_sra.getInvGraphFragment()+"...");
		try{
			is = new FileInputStream(_sra.getInvGraphFragment());
			is = new GZIPInputStream(is); 
			
			input = new NxParser(is);
			input = new ReorderIterator(input, new int[]{1,0});
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
		}
		
		_log.info("...sorting to "+_sra.getGraphFragment()+"...");
		Callback cb = null; OutputStream os = null;
		try{
			os = new FileOutputStream(_sra.getGraphFragment());
			os = new GZIPOutputStream(os); 
			
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
		}
		
		sa = new SortArgs(input);
		
		try{
			SortIterator si = new SortIterator(sa);
			while(si.hasNext())
				cb.processStatement(si.next());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error sorting input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error sorting input file "+_sra.getInvGraphFragment()+" on server "+_serverID+"\n"+e);
		}
		
		try{
			is.close();
			os.close(); 
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing streams on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing streams on server "+_serverID+"\n"+e);
		}
		
		_log.info("Finished preparing graph fragment in "+(System.currentTimeMillis()-b4)+"ms.");
		
		
		
		_log.info("Exporting results...");
		
		RemoteInputStreamServer istream = null;
		
		RemoteInputStream[] riss = new RemoteInputStream[2];
		
		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sra.getGraphFragment())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			_log.info("...stream created.");
			riss[0] = result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		}
		
		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(_sra.getInvGraphFragment())));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
//			addToClose(istream);
			istream = null;
			
			_log.info("...inv stream created.");
			riss[1] = result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		}
		
		return riss;
	}
	
	public void rankAndScatterTriples(String allRanks) throws RemoteException {
		_log.info("Ranking and scattering triples...");
		long b4 = System.currentTimeMillis();
		
		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(_sra.getSortedByContext());
			is = new GZIPInputStream(is); 
			
			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getSortedByContext()+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+_sra.getSortedByContext());
		
		InputStream is2 = null;
		Iterator<Node[]> ranks = null;
		try{
			is2 = new FileInputStream(allRanks);
			is2 = new GZIPInputStream(is2); 
			
			ranks = new NxParser(is2);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+allRanks+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+allRanks+" on server "+_serverID+"\n"+e);
		}
		_log.info("...ranks from "+allRanks);
		
		RankTriplesIterator rti = new RankTriplesIterator(input, ranks);
		
		SortArgs sa = new SortArgs(rti);
		sa.setComparator(new NodeComparator(true, true));
		sa.setTicks(TICKS);
		sa.setTmpDir(_sra.getTmpDir());
		
		try{
			SortIterator si = new SortIterator(sa);
//			AggregateTripleRanksIterator atri = new AggregateTripleRanksIterator(si);
			
			RMIUtils.mkdirs(_sra.getOutScatterDir());
			RemoteScatter.scatter(si, _rmic, _sra.getOutScatterDir(), _sra.getRemoteGatherDir());
			
			
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
		}
		
		_log.info("...triples ranked and scattered in "+(System.currentTimeMillis()-b4)+" ms.");
	}
	
	public void aggregateRanks() throws RemoteException {
		_log.info("Aggregating ranked triples...");
		long b4 = System.currentTimeMillis();
		
		InputStream[] iss = new InputStream[_toGatherFn.size()];
		NxParser[] nxps = new NxParser[_toGatherFn.size()];
		
		
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
		
		_log.info("...output to "+_sra.getOutFinal()+"...");
		OutputStream os = null;
		try{
			os = new FileOutputStream(_sra.getOutFinal());
			os = new GZIPOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+_sra.getOutFinal()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+_sra.getOutFinal()+" on server "+_serverID+"\n"+e);
		}
		
		
		
		_log.info("...starting merge sort...");
		MergeSortArgs msa = new MergeSortArgs(nxps);
		msa.setComparator(NO_DUPES);
		msa.setTicks(TICKS);
		
		MergeSortIterator msi = new MergeSortIterator(msa);
		AggregateTripleRanksIterator atri = new AggregateTripleRanksIterator(msi);
		
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
    	RMIAnnRankingServer rmi = new RMIAnnRankingServer();
    	
    	Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}
	
	public static class ExtractContextsCallback implements Callback{
		Callback _main;
		Callback _con;
		Node _old = null;
		int _cons = 0;
		int _all = 0;
		
		public ExtractContextsCallback(Callback mainCallback, Callback contextCallback){
			_main = mainCallback;
			_con = contextCallback;
		}

		public void endDocument() {
			;
		}

		public void processStatement(Node[] na) {
			_main.processStatement(na);
			_all++;
			if(_old==null || !na[3].equals(_old)){
				_con.processStatement(new Node[]{na[3]});
				_cons++;
			}
			_old = na[3];
		}
		
		public int getAllProcessed(){
			return _all;
		}
		
		public int getConsProcessed(){
			return _cons;
		}

		public void startDocument() {
			;
		}
	}
}
