package org.semanticweb.swse.cons2;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons2.utils.ConsolidationIterator;
import org.semanticweb.swse.cons2.utils.ConsolidationIterator.HandleSameAs;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
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
public class RMIConsolidationServer implements RMIConsolidationInterface {
	private final static Logger _log = Logger.getLogger(RMIConsolidationServer.class.getSimpleName());
	private final static int TICKS = 10000000;
	
	
	public static final Node[] IFP_BLACKLIST = { 
		new Literal("08445a31a78661b5c746feff39a9db6e4e2cc5cf"),
		new Literal("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
		new Literal(""),
		new Literal("N/A"),
		new Literal("n/a"),
		new Literal("none"),
		new Literal("ask"),
		new Literal("blah"),
		new Literal("no"),
		new Literal("n"),
		new Literal("N"),
		new Resource(""),
		new Resource("mailto:"),
		new Resource("http://")
	};
	
	public static final HashSet<Node> IFP_BLACKLIST_HS = new HashSet<Node>();  
	
	static{
		for(Node n:IFP_BLACKLIST){
			IFP_BLACKLIST_HS.add(n);
		}
	}
	
	public final static String IFPS_FPS_FILE = "ifps-fps.nq.gz";
	public final static String CONSOLIDATION_TRIPLES = "cons-triples.gz";
	public final static String OUTPUT_FILE = "data.cons2.nq.gz";
	
	public final static String SORTED_S_FILE = "data.sorted.s.gz";
	public final static String SORTED_O_FILE = "data.sorted.o.gz";
	
	public final static String SAMEAS_FILE = "sameas.final.nq.gz";
	
	public final static String CONS_S_FILE = "data.cons.s.nq.gz";
	public final static String CONS_O_FILE = "data.cons.o.nq.gz";
	
	public final static int[] O_ORDER = new int[]{2,1,0,3};
	
	public final static int S = 0;
	public final static int O = 2;

	private int _serverID = -1;
	private RMIRegistries _servers;
	private String _infile;
	private String _outdir;
	private String _tmpdir;
	private boolean _ingz;
	private String _sorted;
	private String _sameas;
	
	private boolean _sorting = false;
	private boolean _gathering = false;

	public RMIConsolidationServer() throws IOException, ClassNotFoundException{
		;
	}

	public void init(int serverId, RMIRegistries servers, String infile, boolean gzip, String outdir, String tmpdir) throws RemoteException {
		_log.log(Level.INFO, "Initialising server "+serverId+".");

		_infile = infile;
		
		
		_ingz = gzip;

		_outdir = outdir;
		RMIUtils.mkdirs(outdir);
		
		_tmpdir = tmpdir;
		RMIUtils.mkdirs(tmpdir);

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public RemoteInputStream getIFPsandFPs(boolean reason) throws RemoteException {
		NxParser nxp = null;
		InputStream is = null;
		
		try{
			is = new FileInputStream(_infile);
			if(_ingz){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_infile+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_infile+" on server "+_serverID+"\n"+e);
		}

		Callback cs = null;
		String out = _outdir+"/"+IFPS_FPS_FILE;
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(out));
			cs = new CallbackNxOutputStream(os);
			extractIFPsandFPs(nxp, reason, cs);
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating ifps/fps file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating ifps/fps file on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(out)));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
		}
	}
	
	public RemoteInputStream getConsolidationTriples(Set<Node> ifps, Set<Node> fps) throws RemoteException {
		NxParser nxp = null;
		InputStream is = null;
		
		try{
			is = new FileInputStream(_infile);
			if(_ingz){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_infile+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_infile+" on server "+_serverID+"\n"+e);
		}

		Callback cs = null;
		String out = _outdir+"/"+CONSOLIDATION_TRIPLES;
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(out));
			cs = new CallbackNxOutputStream(os);
			extractConsolidationTriples(ifps, fps, nxp, cs);
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating ifps/fps file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating ifps/fps file on server "+_serverID+"\n"+e);
		}

		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(out)));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
		}
	}
	
	public void sort() throws RemoteException {
		_sorting = true;
		String sorted = _outdir+"/"+SORTED_S_FILE;
		
		sort(_infile, _ingz, _tmpdir, sorted);
		
		_sorted = sorted;
	}
	
	private static void sort(String input, boolean gz, String tmpdir, String output) throws RemoteException {
		sort(input, gz, tmpdir, output, NodeComparator.NC);
	}
	
	private static void sort(String input, boolean gz, String tmpdir, String output, Comparator<Node[]> nc) throws RemoteException {
		_log.info("Sorting input file "+input+"...");
		
		NxParser nxp = null;
		InputStream is = null;
		try{
			is = new FileInputStream(input);
			if(gz){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+input+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+input+"\n"+e);
		}
		
		long b4 = System.currentTimeMillis();
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(output));
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+output+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+output+"\n"+e);
		}
		
		Callback cb = new CallbackNxOutputStream(os);
		
		SortArgs sa = new SortArgs(nxp);
		sa.setTicks(TICKS);
		sa.setTmpDir(tmpdir);
		sa.setComparator(nc);
		
		SortIterator si = null;
		try{
			si = new SortIterator(sa);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error sorting file "+input+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error sorting file "+input+"\n"+e);
		}
		
		while(si.hasNext()){
			cb.processStatement(si.next());
		}
		
		try{
			is.close();
			os.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files.\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files.\n"+e);
		}
		
		_log.info("...file sorted in "+(System.currentTimeMillis()-b4)+" ms. Found "+si.duplicates()+" duplicates from "+si.count()+" triples.");
	}
	
	public void gatherSameAs(RemoteInputStream inFile) throws RemoteException {
		_log.info("Gathering sameAs index...");
		_gathering = true;
		
		InputStream is = null;
		String sameas = _outdir+"/"+SAMEAS_FILE;
		
		long b4 = System.currentTimeMillis();
		
		try{
			is = RemoteInputStreamClient.wrap(inFile);
			is = new GZIPInputStream(is);

			NxParser nxp = new NxParser(is);

			Callback cb = null;
			OutputStream os = null;
			os = new GZIPOutputStream(new FileOutputStream(sameas));
			cb = new CallbackNxOutputStream(os);
			
			while(nxp.hasNext()){
				cb.processStatement(nxp.next());
			}
			
			is.close();
			os.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error gathering file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error gathering file on server "+_serverID+"\n"+e);
		} 
		
		_log.info("...sameas gathered in "+(System.currentTimeMillis()-b4)+" ms.");
		
		_sameas = sameas;
	}
	
	public void consolidate() throws RemoteException{
		_log.info("Consolidating file...");
		long b4 = System.currentTimeMillis();
		
		if(_sorted==null){
			if(_sorting == true){
				//wait ten seconds
				while(_sorted!=null){
					_log.info("Waiting for sort to finish...");
					synchronized(this){
						try {
							this.wait(10000);
						} catch (InterruptedException e) {
							throw new RemoteException("Error waiting on server "+_serverID+".\n"+e);
						}
					}
				}
			} else{
				sort();
			}
		}
		
		if(_sameas==null){
			if(_gathering == true){
				//wait ten seconds
				while(_sameas!=null){
					_log.info("Waiting for gatherSameAs to finish...");
					synchronized(this){
						try {
							this.wait(10000);
						} catch (InterruptedException e) {
							throw new RemoteException("Error waiting on server "+_serverID+".\n"+e);
						}
					}
				}
			} else{
				throw new RemoteException("Need to call gatherSameAs before consolidation!");
			}
		}
		
		long t1 = System.currentTimeMillis();
		_log.info("...prepared in "+(t1-b4)+" ms...");
		
		String cons_s = _outdir +"/"+ CONS_S_FILE;
		Callback cb = null;
		OutputStream os = null;
		InputStream is = null, sais = null;
		NxParser nxp = null, sanxp = null;
		
		try{
			is = new GZIPInputStream(new FileInputStream(_sorted));
			nxp = new NxParser(is);
			
			sais = new GZIPInputStream(new FileInputStream(_sameas));
			sanxp = new NxParser(sais);
			
			os = new GZIPOutputStream(new FileOutputStream(cons_s));
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening files on server "+_serverID+"\n"+e);
		} 
		
		rewrite(nxp, sanxp, cb, S);
		long t2 = System.currentTimeMillis();
		_log.info("...consolidated subjects of data in "+(t2-t1)+" ms -- total: "+(b4-t2));
		
		try{
			os.close();
			is.close();
			sais.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files on server "+_serverID+"\n"+e);
		} 
		
		String cons_o = _outdir +"/"+ CONS_O_FILE;
		cb = null; os = null; 
		is = null; nxp = null;  
		sais = null; sanxp = null;
		
		_log.info("...sorting output by O.");
		
		String sorted_o = _outdir +"/"+ SORTED_O_FILE;
		
		NodeComparator nc_o = new NodeComparator(O_ORDER);
		sort(cons_s, true, _tmpdir, sorted_o, nc_o);
		long t3 = System.currentTimeMillis();
		_log.info("...sorted data by object in "+(t3-t2)+" ms -- total "+(t3-b4));
		
		try{
			is = new GZIPInputStream(new FileInputStream(sorted_o));
			nxp = new NxParser(is);
			
			sais = new GZIPInputStream(new FileInputStream(_sameas));
			sanxp = new NxParser(sais);
			
			os = new GZIPOutputStream(new FileOutputStream(cons_o));
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening files on server "+_serverID+"\n"+e);
		} 
		
		rewrite(nxp, sanxp, cb, O);
		long t4 = System.currentTimeMillis();
		_log.info("...consolidated objects of data in "+(t4-t3)+" ms -- total "+(t4-b4));
		
		
		try{
			os.close();
			is.close();
			sais.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files on server "+_serverID+"\n"+e);
		} 
		_log.info("...finished consolidation in total "+(t4-b4)+" ms");
	}
	
	public static void rewrite(Iterator<Node[]> data, Iterator<Node[]> sameAs, Callback cb, int pos) throws RemoteException {
		_log.info("Consolidating file...");
		long b4 = System.currentTimeMillis();
		
		ConsolidationIterator ci = new ConsolidationIterator(data, sameAs, HandleSameAs.FILTER, pos);
		
		while(ci.hasNext()){
			cb.processStatement(ci.next());
		}
		
		long t1 = System.currentTimeMillis();
		
		_log.info("...consolidated "+(t1-b4)+" ms. Scanned "+ci.count()+" stmts. Filtered "+ci.filtered()+" stmts. Rewrote "+ci.rewrittenStmts()+" statements. Rewrote "+ci.rewrittenIDs()+" ids.");
	}

	private void extractConsolidationTriples(Set<Node> ifps, Set<Node> fps, Iterator<Node[]> nxp, Callback cs) {
		Node[] next;
		while(nxp.hasNext()){
			next = nxp.next();
			if(next[1].equals(OWL.SAMEAS)){
				cs.processStatement(next);
			} else if(ifps!=null && ifps.contains(next[1])){
				cs.processStatement(next);
			} else if(fps!=null && fps.contains(next[1])){
				cs.processStatement(next);
			}
		}
	}
	
	private void extractIFPsandFPs(Iterator<Node[]> nxp, boolean reason, Callback cs) {
		Node[] next;
		while(nxp.hasNext()){
			next = nxp.next();
			if(next[1].equals(RDF.TYPE)){ 
				if(next[2].equals(OWL.INVERSEFUNCTIONALPROPERTY) || next[2].equals(OWL.FUNCTIONALPROPERTY)){
					cs.processStatement(next);
				} else if(reason && next[2].equals(OWL.SYMMETRICPROPERTY)){
					cs.processStatement(next);
				}
			} else if(reason){
				if(next[1].equals(RDFS.SUBPROPERTYOF) || next[1].equals(OWL.INVERSEOF) || next[1].equals(OWL.EQUIVALENTPROPERTY)){
					cs.processStatement(next);
				}
			}
		}
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIConsolidationServer rmi = new RMIConsolidationServer();
    	
    	RMIConsolidationInterface stub = (RMIConsolidationInterface) UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		;
	}
}
