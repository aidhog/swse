package org.semanticweb.swse.econs.sim.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.econs.sim.RMIEconsSimInterface;
import org.semanticweb.swse.econs.sim.master.RemoteOutputStreamThread;
import org.semanticweb.swse.file.master.LocalWriteRemoteStreamThread;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteOutputStreamClient;

public class RemoteScatter {
	static Logger _log = Logger.getLogger(RemoteScatter.class.getName());
	public final static int TICKS = 1000000;
	public final static String TEMP = "temp";
	public final static String GATHER = "gather.nq.gz";
	public final static String SCATTER = "scatter.nq.gz";

	public static void scatter(Iterator<Node[]> iter, RMIClient<RMIEconsSimInterface> rmic, String loutdir, String routdir) throws Exception {
		String[] files = split(iter, 0, rmic.getServers(), loutdir);

		RemoteOutputStreamThread[] gost = new RemoteOutputStreamThread[files.length];
		for(int j=0; j<files.length; j++){
			_log.log(Level.INFO, "...sending file "+files[j]+"...");

			RMIEconsSimInterface rmii = rmic.getStub(j);

			String fn = RMIUtils.getLocalName(routdir,j)+"/"+rmic.getServers().thisServerId()+"."+GATHER;
			
			_log.log(Level.INFO, "Getting remote output stream from "+rmic.getServers().thisServerId()+" to "+j+" for ..."+fn);

			gost[j] = new RemoteOutputStreamThread(rmii, j, fn);
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

		_log.log(Level.INFO, "...remote file scatter done of local data.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on sending file "+idletime+"...");
		_log.info("Average idle time for co-ordination on sending file "+(double)idletime/(double)(ibts.length)+"...");
	}

	public static String[] split(Iterator<Node[]> in, int el, RMIRegistries servers, String outdir) throws FileNotFoundException, IOException, ParseException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		for(int i=0; i<files; i++){
			String outfile = outdir+"/"+i+"."+SCATTER;
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
		return fns;
	}
}
