package org.semanticweb.swse.ann.agg.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.ann.agg.SlaveAnnAggregateArgs;
import org.semanticweb.swse.ann.rank.RMIAnnRankingServer;

public class MasterAnnAggregatorArgs extends MasterArgs<MasterAnnAggregatorArgs>{
	public static final RMIAnnRankingServer DEFAULT_RI = new RMIAnnRankingServer();
	public static final MasterAnnAggregator DEFAULT_MASTER = new MasterAnnAggregator();
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TEMP_DIR+"scatter/";
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather/";
	
	
	private boolean _gzlocal = DEFAULT_GZ_OUT;
	private String _outdir = null;
	private String _local = null;
	private String _tmpdir = null;
	private SlaveAnnAggregateArgs _sra = null;
	private String _rGatherDir = null;
	private String _scatterDir = null;
	
	public MasterAnnAggregatorArgs(String local, String outdir, String remoteGatherDir, SlaveAnnAggregateArgs sra){
		_outdir = outdir;
		_local = local;
		_sra = sra;
		_rGatherDir = remoteGatherDir;
		initDefaults(outdir);
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
	}
	
	public String getRemoteGatherDir(){
		return _rGatherDir;
	}
	
	public String getScatterDir(){
		return _scatterDir;
	}
	
	public void setGzLocal(boolean gzL){
		_gzlocal = gzL;
	}
	
	public boolean getGzLocal(){
		return _gzlocal;
	}
	
	public String getLocal(){
		return _local;
	}
	
	public void setTmpDir(String tmpDir){
		_tmpdir = tmpDir;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public SlaveAnnAggregateArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public SlaveAnnAggregateArgs getSlaveArgs(){
		return _sra;
	}
	
	public String toString(){
		return "Master:: local:"+_local+" gzlocal:"+_gzlocal+" tmpdir:"+_tmpdir+"\n"
			+"Slave:: "+_sra;
	}
	
	public RMIAnnRankingServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterAnnAggregator getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TEMP_DIR;
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return outdir+"/"+DEFAULT_SCATTER_DIR;
	}
}