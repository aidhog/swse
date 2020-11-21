package org.semanticweb.swse.index2.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.file.master.LocalWriteRemoteStreamThread;
import org.semanticweb.swse.index2.RMIIndexerInterface;
import org.semanticweb.swse.index2.master.RemoteIndexerGetOutputStreamThread;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteOutputStreamClient;

public class RemoteScatter {
	static Logger _log = Logger.getLogger(RemoteScatter.class.getName());
	public final static int TICKS = 1000000;
	public final static String TEMP = "temp";

	public static void scatter(String[] infile, boolean[] gzip, RMIClient<RMIIndexerInterface> rmic, String outdir, String filesuffix) throws Exception {
		if(infile.length!=gzip.length){
			throw new RemoteException("Need a gzip entry for every infile arg!");
		}

		for(int i=0; i<infile.length; i++){
			_log.info("Scattering "+infile[i]+" to remote servers...");
			String[] files = null;
			
			InputStream is = new FileInputStream(infile[i]);
			if(gzip[i]){
				is = new GZIPInputStream(is);
			}
			NxParser nxp = new NxParser(is);

			files = sortAndSplit(nxp, 0, rmic.getServers(), outdir, i+"."+filesuffix);

			RemoteIndexerGetOutputStreamThread[] gost = new RemoteIndexerGetOutputStreamThread[files.length];
			for(int j=0; j<files.length; j++){
				_log.log(Level.INFO, "...sending file "+files[j]+"...");

				RMIIndexerInterface rmii = rmic.getStub(j);

				_log.log(Level.INFO, "Getting remote output stream...");

				gost[j] = new RemoteIndexerGetOutputStreamThread(rmii, j);
				gost[j].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int j=0; j<gost.length; j++){
				gost[j].join();
				if(!gost[j].successful()){
					throw gost[j].getException();
				}
				_log.log(Level.INFO, "..."+j+" gotten output stream...");
			}

			_log.log(Level.INFO, "...remote outputstream opened.");
			long idletime = RMIThreads.idleTime(gost);
			_log.info("Total idle time for co-ordination on opening remote outputstream "+idletime+"...");
			_log.info("Average idle time for co-ordination on opening remote outputstream "+(double)idletime/(double)(gost.length)+"...");

			_log.log(Level.INFO, "...writing to remote output stream(s)...");

			VoidRMIThread[] ibts = new VoidRMIThread[gost.length];
			for(int j=0; j<ibts.length; j++){
				ibts[j] = new LocalWriteRemoteStreamThread(new FileInputStream(files[j]), RemoteOutputStreamClient.wrap(gost[j].getResult()), j);
				ibts[j].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int j=0; j<ibts.length; j++){
				ibts[j].join();
				if(!ibts[j].successful()){
					throw ibts[j].getException();
				}
				_log.log(Level.INFO, "..."+j+" written output stream...");
			}

			_log.log(Level.INFO, "...remote file scatter done of local file "+infile[i]+".");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sending file "+idletime+"...");
			_log.info("Average idle time for co-ordination on sending file "+(double)idletime/(double)(ibts.length)+"...");

		}
	}

	public static String[] sortAndSplit(Iterator<Node[]> in, int el, RMIRegistries servers, String outdir, String filesuffix) throws FileNotFoundException, IOException, ParseException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		for(int i=0; i<files; i++){
			String outfile = outdir+"/"+i+"."+filesuffix;
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
			int server = servers.getServerNo(next[el]);
			cb[server].processStatement(next);
		}

		for(int i=0; i<files; i++){
			os[i].close();
		}
		_log.info("...splitting file done in "+(System.currentTimeMillis()-b4)+" ms... split "+c+" statements.");
		
		_log.info("Sorting split files...");
		String tempDir = outdir+TEMP;
		for(int i=0; i<files; i++){
			InputStream is = new GZIPInputStream(new FileInputStream(fns[i]));
			NxParser nxp = new NxParser(is);
			SortArgs sa = new SortArgs(nxp);
			sa.setTicks(TICKS);
			sa.setTmpDir(tempDir);
		
			SortIterator si = new SortIterator(sa);
			RemoveReasonedDupesIterator rrdi = new RemoveReasonedDupesIterator(si);
			
			String outfile = outdir+"/"+i+".s."+filesuffix;
			fns[i] = outfile;
			OutputStream sos = new GZIPOutputStream(new FileOutputStream(outfile));
			CallbackNxOutputStream scb = new CallbackNxOutputStream(sos);
			
			while(rrdi.hasNext()){
				scb.processStatement(rrdi.next());
			}
			
			is.close();
			sos.close();
			
			_log.info("...removed "+rrdi.duplicatesRemoved()+" reasoned duplicates...");
			_log.info("...sorted file "+outfile+" in "+(System.currentTimeMillis()-b4)+" ms...");
		}
		_log.info("...sorting split files done in "+(System.currentTimeMillis()-b4)+" ms... split "+c+" statements.");
		return fns;
	}
}
