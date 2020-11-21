package org.semanticweb.swse.bench.master;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.bench.RMIBenchConstants;
import org.semanticweb.swse.bench.RMIBenchInterface;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterBencher {
	private final static Logger _log = Logger.getLogger(MasterBencher.class.getSimpleName());
	public static final String GATHER_FILE = "gather.nq.gz";
	
	public static final String SPLIT_FILE = "split.nq.gz";

	private RMIRegistries _servers;
	private RMIClient<RMIBenchInterface> _rmic;
	
	private ArrayList<RemoteInputStream> _ris = null;

	public MasterBencher(RMIRegistries servers) throws RemoteException, NotBoundException{
		_servers = servers;
		_rmic = new RMIClient<RMIBenchInterface>(servers, RMIBenchConstants.DEFAULT_STUB_NAME);
	}

	public void coordinate(String infile, boolean gzip, String outdir) throws Exception{
		_log.info("Starting bench...");
		
		Collection<RMIBenchInterface> stubs = _rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising co-ordinate...");
		Iterator<RMIBenchInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteBenchInitThread(stubIter.next(), i, _servers, RMIUtils.getLocalName(outdir, i));
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" initialised...");
		}
		_log.log(Level.INFO, "...remote indexers initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Scattering remote...");

		stubIter = stubs.iterator();

		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteBenchScatterThread(stubIter.next(), i, RMIUtils.getLocalName(infile, i), gzip);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return for remote scatter...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
		}
		_log.log(Level.INFO, "...remote threads scattered received.");

		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Closing...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteBenchCloseThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" index built...");
		}
		_log.log(Level.INFO, "...remote index build done.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on bench "+idletime+"...");
		_log.info("Average idle time for co-ordination on bench "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "...bench finished.");
	}
	
	public void gatherScatter(String infile, boolean gzip, String outdir) throws Exception{
		_log.info("Starting bench...");
		
		Collection<RMIBenchInterface> stubs = _rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising gather scatter...");
		Iterator<RMIBenchInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteBenchInitThread(stubIter.next(), i, _servers, RMIUtils.getLocalName(outdir, i));
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" initialised...");
		}
		_log.log(Level.INFO, "...remote indexers initialised.");
		double idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on initialising "+idletime+"...");
		_log.info("Average idle time for co-ordination on initialising "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Gathering remote...");

		stubIter = stubs.iterator();

		RemoteBenchGatherLocalThread[] gathers = new RemoteBenchGatherLocalThread[ibts.length];
		InputStream[] iss = new InputStream[gathers.length];
		for(int i=0; i<ibts.length; i++){
			gathers[i] = new RemoteBenchGatherLocalThread(stubIter.next(), i, RMIUtils.getLocalName(infile, i), gzip);
			gathers[i].start();
			
		}
		
		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<gathers.length; i++){
			gathers[i].join();
			if(!gathers[i].successful()){
				throw gathers[i].getException();
			}
			iss[i] = RemoteInputStreamClient.wrap(gathers[i].getResult());
			_log.log(Level.INFO, "..."+i+" stream initialised...");
		}
		
		
		long b4 = System.currentTimeMillis();
		_log.log(Level.INFO, "Locally aggregating data...");
		NxParser nxp[] = new NxParser[iss.length];
		for(int i=0; i<nxp.length; i++){
			nxp[i] = new NxParser(new GZIPInputStream(iss[i]));
		}
		
		RMIUtils.mkdirs(RMIUtils.getLocalName(outdir));
		
		String cachefile = RMIUtils.getLocalName(outdir)+"/"+GATHER_FILE;
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(cachefile)); 
		CallbackNxOutputStream cnqos = new CallbackNxOutputStream(os);
		
		int c = 0;
		boolean done = false;
		while(!done){
			done = true;
			for(Iterator<Node[]> i:nxp){
				if(i.hasNext()){
					c++;
					done = false;
					cnqos.processStatement(i.next());
				}
			}
		}
		os.close();
		
		for(InputStream is:iss){
			is.close();
		}
		
		_log.log(Level.INFO, "...gathered data in "+(System.currentTimeMillis()-b4)+" ms.");
		
		b4 = System.currentTimeMillis();
		
		_log.log(Level.INFO, "Scattering data...");
		_log.info("Scattering "+cachefile+" to remote servers...");
		String[] files = null;
		try{
			InputStream is = new FileInputStream(cachefile);
			is = new GZIPInputStream(is);
					
			NxParser nx = new NxParser(is);

			files = split(nx, RMIUtils.getLocalName(outdir), SPLIT_FILE, 0);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error splitting local file on master server\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error splitting local file on master server\n"+e);
		}
		
		
		RemoteBenchGatherThread[] gatherTs = new RemoteBenchGatherThread[files.length];
		_ris = new ArrayList<RemoteInputStream>();
		for(int j=0; j<files.length; j++){
			RemoteInputStreamServer istream = null;

			try {
				istream = new SimpleRemoteInputStream(new BufferedInputStream(
						new FileInputStream(files[j])));
				RemoteInputStream result = istream.export();
					istream = null;
					
				_ris.add(istream);

				_log.info("Scattering "+files[j]+" to remote server");
				RMIBenchInterface rmii = _rmic.getStub(j);

				RemoteBenchGatherThread rigt = new RemoteBenchGatherThread(rmii, j, result);

				rigt.start();

				gatherTs[j] = rigt;
			} catch(IOException e){ 
				_log.log(Level.SEVERE, "Error creating RemoteInputStream on master server\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error creating RemoteInputStream on master server\n"+e);
			}
			
			_log.info("Scattering local file took "+(System.currentTimeMillis()-b4)+" ms.");
		}

		for(int i=0; i<gatherTs.length; i++){
			gatherTs[i].join();
			if(!gatherTs[i].successful()){
				throw gatherTs[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" sent...");
		}
		_log.log(Level.INFO, "...data sent initialised.");

		idletime = RMIThreads.idleTime(gatherTs);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(gatherTs.length)+"...");

		_log.log(Level.INFO, "Closing...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteBenchCloseThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" index built...");
		}
		_log.log(Level.INFO, "...remote index build done.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on indexing "+idletime+"...");
		_log.info("Average idle time for co-ordination on indexing "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "...distributed indexing finished.");
		
		for(RemoteInputStream ris:_ris){
			if(ris!=null){
				ris.close(true);
			}
		}
	}

	private String[] split(Iterator<Node[]> in, String outdir, String fn, int el) throws FileNotFoundException, IOException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = _servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		RMIUtils.mkdirs(outdir);

		int c = 0;
		for(int i=0; i<files; i++){
			String outfile = outdir+"/"+i+"."+fn;
			fns[i] = outfile;
			os[i] = new GZIPOutputStream(new FileOutputStream(outfile));
			cb[i] = new CallbackNxOutputStream(os[i]);
		}

		while(in.hasNext()){
			c++;
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
}
