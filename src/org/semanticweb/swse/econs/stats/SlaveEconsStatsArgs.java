package org.semanticweb.swse.econs.stats;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveEconsStatsArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final String DEFAULT_TMP_DIR = "tmp/";
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TMP_DIR+"scatter/";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.final.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather/";
	
	public static final String DEFAULT_LOCAL_GATHER_DIR = "gall/";
	
	private String _in;
	private boolean _gzIn = MasterArgs.DEFAULT_GZ_IN;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _localGatherDir;
	
	public SlaveEconsStatsArgs(String in, String outdir){
		_in = in;
		_outdir = outdir;
		initDefaults(outdir);
	}
	
	public String getOutDir(){
		return _outdir;
	}
	
	public String getIn(){
		return _in;
	}
	
	public void setGzIn(boolean gzin){
		_gzIn = gzin;
	}
	
	public boolean getGzIn(){
		return _gzIn;
	}
	
	public void setTmpDir(String tmpDir){
		_tmpdir = tmpDir;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public void setScatterDir(String sDir){
		_scatterDir = sDir;
	}
	
	public String getOutScatterDir(){
		return _scatterDir;
	}

	public void setRemoteGatherDir(String gDir){
		_remoteGatherDir = gDir;
	}
	
	public String getRemoteGatherDir(){
		return _remoteGatherDir;
	}
	
	public void setLocalGatherDir(String gDir){
		_localGatherDir = gDir;
	}
	
	public String getLocalGatherDir(){
		return _localGatherDir;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
		_localGatherDir = getDefaultLocalGatherDir(outdir);
	}
	
	public SlaveEconsStatsArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String scatterDir = RMIUtils.getLocalName(_scatterDir, server);
		String localGatherDir = RMIUtils.getLocalName(_localGatherDir, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveEconsStatsArgs saa = new SlaveEconsStatsArgs(in, out);
		saa.setGzIn(_gzIn);
		//don't localise
		saa.setRemoteGatherDir(_remoteGatherDir);
		saa.setScatterDir(scatterDir);
		saa.setSlaveLog(log);
		saa.setTmpDir(tmpdir);
		saa.setLocalGatherDir(localGatherDir);
		return saa;
	}
	
	public String toString(){
		return "in:"+_in+" gzIn:"+_gzIn+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir+" localGatherDir:"+_localGatherDir;
	}
	
	public static final String getDefaultOutFinalFilename(String outdir){
		return getDefaultOutFinalFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutFinalFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_NGZ;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TMP_DIR;
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return outdir+"/"+DEFAULT_SCATTER_DIR;
	}
	
	private static String getDefaultRemoteGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_REMOTE_GATHER_DIR;
	}
	
	private static String getDefaultLocalGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_LOCAL_GATHER_DIR;
	}
}
