package org.semanticweb.swse.file.master;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.Master;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.file.RMIFileInterface;

import com.healthmarketscience.rmiio.DirectRemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterFile implements Master<MasterFileArgs>{
	private final static Logger _log = Logger.getLogger(MasterFile.class.getSimpleName());

	public MasterFile(){
		;
	}

	public void startRemoteTask(RMIRegistries servers, String stubName, MasterFileArgs mla) throws Exception{
		RMIClient<RMIFileInterface> rmic = new RMIClient<RMIFileInterface>(servers, stubName);

		RMIUtils.setLogFile(mla.getMasterLog());

		_log.info("Starting file copy...");

		Collection<RMIFileInterface> stubs = rmic.getAllStubs();
		RMIThread<? extends Object>[] ibts = new RMIThread[stubs.size()];

		_log.log(Level.INFO, "Initialising remote file server...");
		Iterator<RMIFileInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteFileInitThread(stubIter.next(), i, servers, mla.getSlaveArgs(i));
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

		if(mla.getUseROutputStream()){
			_log.log(Level.INFO, "Running send file...");

			stubIter = stubs.iterator();
			
			_log.log(Level.INFO, "Getting remote output stream...");

			RMIThread<RemoteOutputStream>[] gost = new RMIThread[stubs.size()];
			
			for(int i=0; i<gost.length; i++){
				gost[i] = new RemoteFileGetOutputStreamThread(stubIter.next(), i, RMIUtils.getLocalName(mla.getRemoteFile(), i));
				gost[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<gost.length; i++){
				gost[i].join();
				if(!gost[i].successful()){
					throw gost[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" gotten output stream...");
			}
			
			_log.log(Level.INFO, "...remote outputstream opened.");
			idletime = RMIThreads.idleTime(gost);
			_log.info("Total idle time for co-ordination on opening remote outputstream "+idletime+"...");
			_log.info("Average idle time for co-ordination on opening remote outputstream "+(double)idletime/(double)(ibts.length)+"...");
			
			_log.log(Level.INFO, "...writing to remote output stream(s)...");
			
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new LocalWriteRemoteStreamThread(new FileInputStream(mla.getLocalFile()), RemoteOutputStreamClient.wrap(gost[i].getResult()), i);
				ibts[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" written output stream...");
			}
			
			
			_log.log(Level.INFO, "...remote file send done.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sending file "+idletime+"...");
			_log.info("Average idle time for co-ordination on sending file "+(double)idletime/(double)(ibts.length)+"...");
		} else{
			_log.log(Level.INFO, "Running send file...");

			stubIter = stubs.iterator();

			ArrayList<InputStream> toClose = new ArrayList<InputStream>();
			for(int i=0; i<ibts.length; i++){
				InputStream fileData = new FileInputStream(mla.getLocalFile());
				toClose.add(fileData);
				//			RemoteInputStreamServer remoteFileData = new SimpleRemoteInputStream(fileData);
				//			RemoteInputStream ris = remoteFileData.export();
				ibts[i] = new RemoteSendFileThread(stubIter.next(), i, new DirectRemoteInputStream(fileData), RMIUtils.getLocalName(mla.getRemoteFile(), i));
				ibts[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" file written...");
			}
			_log.log(Level.INFO, "...remote file send done.");
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on sending file "+idletime+"...");
			_log.info("Average idle time for co-ordination on sending file "+(double)idletime/(double)(ibts.length)+"...");
			for(InputStream is:toClose)
				is.close();
		}

		rmic.clear();

		_log.log(Level.INFO, "...distributed file sending finished.");

	}
}
