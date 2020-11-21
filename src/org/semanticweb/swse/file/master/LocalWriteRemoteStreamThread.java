package org.semanticweb.swse.file.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;

/**
 * Cheating... doesn't really call a remote method :O
 * @author aidhog
 *
 */
public class LocalWriteRemoteStreamThread extends VoidRMIThread {
	Exception _e;
	InputStream _is; OutputStream _os;
	
	public LocalWriteRemoteStreamThread(InputStream is, OutputStream os, int server){
		super(server);
		_is = is;
		_os = os;
	}

	protected void runRemoteVoidMethod() throws RemoteException {
		// Transfer bytes from in to out
		try{
		    byte[] buf = new byte[1024];
		    int len;
		    while ((len = _is.read(buf)) > 0) {
		    	_os.write(buf, 0, len);
		    }
		    _is.close();
		    _os.close();
		} catch(IOException e){
			throw new RemoteException("Error writing to remote output stream!\n"+e.getMessage());
		}
	}
}
