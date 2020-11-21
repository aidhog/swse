package org.semanticweb.swse.index.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.index.RMIIndexerServer;
import org.semanticweb.swse.index.SlaveIndexerArgs;

public class MasterIndexerArgs extends MasterArgs<MasterIndexerArgs>{
	
	public static final String TMP_DIR = "idxtmp/";
	public static final String DEFAULT_SCATTER_DIR = TMP_DIR+"scatter/";
	
	private final String[] _localfiles;
	private boolean[] _localgzip;
	
	private final String[] _remotefiles;
	private boolean[] _remotegzip;
	
	private final String _scatterdir;
	
	private SlaveIndexerArgs _sia = null;
	
	public static final RMIIndexerServer DEFAULT_RI = new RMIIndexerServer();
	public static final MasterIndexer DEFAULT_MASTER = new MasterIndexer();
	
	
	public MasterIndexerArgs(String[] localfiles, String[] remotefiles, String scatterdir, SlaveIndexerArgs sia){
		_localfiles = localfiles;
		_remotefiles = remotefiles;
		_scatterdir = scatterdir;
		
		_remotegzip = new boolean[remotefiles.length];
		for(int i=0; i<_remotegzip.length; i++)
			_remotegzip[i] = MasterArgs.DEFAULT_GZ_IN;
		
		_localgzip = new boolean[remotefiles.length];
		for(int i=0; i<_localgzip.length; i++)
			_localgzip[i] = MasterArgs.DEFAULT_GZ_IN;
		
		_sia = sia;
	}
	
	public void setGzRemote(boolean[] gzremote){
		_remotegzip = gzremote;
	}
	
	public boolean[] getGzRemote(){
		return _remotegzip;
	}
	
	public void setGzLocal(boolean[] gzlocal){
		_localgzip = gzlocal;
	}
	
	public boolean[] getGzLocal(){
		return _localgzip;
	}
	
	public String[] getLocalFiles(){
		return _localfiles;
	}
	
	public String[] getRemoteFiles(){
		return _remotefiles;
	}
	
	public String getScatterDir(){
		return _scatterdir;
	}
	
	public SlaveIndexerArgs getSlaveArgs(int server){
		return _sia.instantiate(server);
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append("Master:: ");
		for(int i=0; i<_remotefiles.length; i++){
			buf.append(" remotefile: ["+_remotefiles[i]+" gz:"+_remotegzip[i]+"]");
		}
		for(int i=0; i<_localfiles.length; i++){
			buf.append(" localfile: {"+_localfiles[i]+"-gz:"+_localgzip[i]+"]");
		}
		buf.append(" localscatterdir: "+_scatterdir+"\n");
		buf.append("Slave:: "+_sia);
		
		return buf.toString();
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return RMIUtils.getLocalName(outdir)+DEFAULT_SCATTER_DIR;
	}
	
	public RMIIndexerServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterIndexer getTaskMaster() {
		return DEFAULT_MASTER;
	}
}