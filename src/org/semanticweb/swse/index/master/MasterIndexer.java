package org.semanticweb.swse.index.master;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.index.RMIIndexerInterface;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterIndexer implements Master<MasterIndexerArgs>{
	private final static Logger _log = Logger.getLogger(MasterIndexer.class.getSimpleName());
	public static final String TBOX_REASONING_FILE = "tbox.r.nq.gz";
	public static final String TBOX_FILE = "tbox.nq.gz";
	
	public static final String SPLIT_FILE = "split.nq.gz";

	public MasterIndexer(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterIndexerArgs mia) throws Exception{
		RMIClient<RMIIndexerInterface> rmic = new RMIClient<RMIIndexerInterface>(servers, stubName);
		RMIUtils.setLogFile(mia.getMasterLog());
		
		_log.log(Level.INFO, "Setting up remote indexing job with following args:");
		_log.log(Level.INFO, mia.toString());
		
		Collection<RMIIndexerInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		RMIUtils.mkdirs(mia.getScatterDir());
		
		_log.log(Level.INFO, "Initialising remote indexing...");
		Iterator<RMIIndexerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerInitThread(stubIter.next(), i, servers, mia.getSlaveArgs(i), stubName);
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

		ArrayList<VoidRMIThread> gatherThreads = new ArrayList<VoidRMIThread>();
		if(mia.getLocalFiles()!=null && mia.getLocalFiles().length>0){
			long b4 = System.currentTimeMillis();
			
			_log.log(Level.INFO, "Scattering local file(s) first...");
			for(int i=0; i<mia.getLocalFiles().length; i++){
				_log.info("Scattering "+mia.getLocalFiles()[i]+" to remote servers...");
				String[] files = null;
				try{
					InputStream is = new FileInputStream(mia.getLocalFiles()[i]);
					if(mia.getGzLocal()[i]){
						is = new GZIPInputStream(is);
					}
					NxParser nxp = new NxParser(is);

					files = split(nxp, servers, mia.getScatterDir(), SPLIT_FILE, 0, i);
				} catch(Exception e){
					_log.log(Level.SEVERE, "Error splitting local file on master server\n"+e);
					e.printStackTrace();
					throw new RemoteException("Error splitting local file on master server\n"+e);
				}
				for(int j=0; j<files.length; j++){
					RemoteInputStreamServer istream = null;

					try {
						istream = new SimpleRemoteInputStream(new BufferedInputStream(
								new FileInputStream(files[j])));
						RemoteInputStream result = istream.export();
						istream = null;

						_log.info("Scattering "+files[j]+" to remote server");
						RMIIndexerInterface rmii = rmic.getStub(j);

						RemoteIndexerGatherThread rigt = new RemoteIndexerGatherThread(rmii, j, result);

						rigt.start();

						gatherThreads.add(rigt);
					} catch(IOException e){ 
						_log.log(Level.SEVERE, "Error creating RemoteInputStream on master server\n"+e);
						e.printStackTrace();
						throw new RemoteException("Error creating RemoteInputStream on master server\n"+e);
					} finally {
						// we will only close the stream here if the server fails before
						// returning an exported stream
						if(istream != null) istream.close();
					}
				}
			}
			
			_log.info("scattering local files took "+(System.currentTimeMillis()-b4)+" ms.");
		}
		
		_log.info("..awaiting thread return for scattering local files");
		
		for(VoidRMIThread gatherThread:gatherThreads){
			gatherThread.join();
			if(!gatherThread.successful()){
				throw gatherThread.getException();
			}
			_log.info("..local file scatter thread "+gatherThread.getName()+" finished in "+gatherThread.getTotalTime()+" ms...");
		}
		_log.info("..local files scattered.");
		
		_log.log(Level.INFO, "Scattering remote...");

		stubIter = stubs.iterator();

		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerScatterThread(stubIter.next(), i, RMIUtils.getLocalNames(mia.getRemoteFiles(), i), mia.getGzRemote());
			ibts[i].start();
		}

		
		

		_log.log(Level.INFO, "...awaiting thread return for remote scatter...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" remote scattered...");
		}
		_log.log(Level.INFO, "...remote threads scattered received.");

		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Running remote index build...");
		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteIndexerMakeIndexThread(stubIter.next(), i);
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

		rmic.clear();
		
		_log.log(Level.INFO, "...distributed indexing finished.");
	}

	private String[] split(Iterator<Node[]> in, RMIRegistries servers, String outdir, String fn, int el, int f) throws FileNotFoundException, IOException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		int c = 0;
		for(int i=0; i<files; i++){
			String outfile = outdir+"/"+f+"."+i+"."+fn;
			fns[i] = outfile;
			os[i] = new GZIPOutputStream(new FileOutputStream(outfile));
			cb[i] = new CallbackNxOutputStream(os[i]);
		}

		while(in.hasNext()){
			c++;
			Node[] next = in.next();
			int server = servers.getServerNo(next[el]);
			cb[server].processStatement(next);
		}

		for(int i=0; i<files; i++){
			os[i].close();
		}
		_log.info("...splitting file done in "+(System.currentTimeMillis()-b4)+" ms... split "+c+" statements.");
		return fns;
	}
}
