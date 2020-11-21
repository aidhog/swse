package org.semanticweb.swse.ann.agg;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveAnnAggregateArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final String DEFAULT_TMP_DIR = "tmp/";
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TMP_DIR+"scatter/";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.final.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather/";
	
	private String _r;
	private boolean _gzR = MasterArgs.DEFAULT_GZ_IN;
	
	private String _in;
	private boolean _gzIn = MasterArgs.DEFAULT_GZ_IN;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _outFinal = null;
	
	public SlaveAnnAggregateArgs(String in, String raw, String outdir){
		_in = in;
		_r = raw;
		_outdir = outdir;
		initDefaults(outdir);
	}
	
	public String getRaw(){
		return _r;
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
	
	public void setGzRaw(boolean gzR){
		_gzR = gzR;
	}
	
	public boolean getGzRaw(){
		return _gzR;
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

	public void setOutFinal(String of){
		_outFinal = of;
	}
	
	public String getOutFinal(){
		return _outFinal;
	}
	
	public void setRemoteGatherDir(String gDir){
		_remoteGatherDir = gDir;
	}
	
	public String getRemoteGatherDir(){
		return _remoteGatherDir;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_outFinal = getDefaultOutFinalFilename(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
	}
	
	public SlaveAnnAggregateArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String r = RMIUtils.getLocalName(_r, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String scatterDir = RMIUtils.getLocalName(_scatterDir, server);
		String outfinal = RMIUtils.getLocalName(_outFinal, server);
		
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveAnnAggregateArgs saa = new SlaveAnnAggregateArgs(in, r, out);
		saa.setGzIn(_gzIn);
		saa.setGzRaw(_gzR);
		saa.setOutFinal(outfinal);
		//don't localise
		saa.setRemoteGatherDir(_remoteGatherDir);
		saa.setScatterDir(scatterDir);
		saa.setSlaveLog(log);
		saa.setTmpDir(tmpdir);
		return saa;
	}
	
	public String toString(){
		return "in:"+_in+" gzIn:"+_gzIn+" raw:"+_r+" gzraw:"+_gzR+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir+" outfinal:"+_outFinal;
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
	
	private String getDefaultRemoteGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_REMOTE_GATHER_DIR;
	}
}
