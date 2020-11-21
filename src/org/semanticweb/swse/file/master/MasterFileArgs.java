package org.semanticweb.swse.file.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.file.SlaveFileArgs;
import org.semanticweb.swse.lucene.RMILuceneServer;

public class MasterFileArgs extends MasterArgs<MasterFileArgs>{
	public static final RMILuceneServer DEFAULT_RI = new RMILuceneServer();
	public static final MasterFile DEFAULT_MASTER = new MasterFile();
	
	private SlaveFileArgs _sla = null;
	private String _localFile = null;
	private String _remoteFile = null;
	
	private boolean _os = false;
	
	public MasterFileArgs(SlaveFileArgs sla, String localFile, String remoteFile){
		_sla = sla;
		_localFile = localFile;
		_remoteFile = remoteFile;
	}
	
	public SlaveFileArgs getSlaveArgs(int server){
		return _sla.instantiate(server);
	}
	
	public String getRemoteFile(){
		return _remoteFile;
	}
	
	public String getLocalFile(){
		return _localFile;
	}
	
	public boolean getUseROutputStream(){
		return _os;
	}
	
	public void setUseROutputStream(boolean os){
		_os = os;
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append("Master:: from:"+_localFile+" to:"+_remoteFile+" os:"+_os);
		buf.append("Slave:: "+_sla);
		
		return buf.toString();
	}
	
	public RMILuceneServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterFile getTaskMaster() {
		return DEFAULT_MASTER;
	}
}