package org.semanticweb.swse.hobo.stats;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveHoboStatsArgs extends SlaveArgs {
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
	
	private String _r;
	private boolean _gzR = MasterArgs.DEFAULT_GZ_IN;
	
	private String _c;
	private boolean _gzC = MasterArgs.DEFAULT_GZ_IN;
	
	private String _a;
	private boolean _gzA = MasterArgs.DEFAULT_GZ_IN;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _localGatherDir;
	
	private String _statsOut;
	
	public SlaveHoboStatsArgs(String in, String outdir){
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
	
	public void setGzA(boolean gza){
		_gzA = gza;
	}
	
	public boolean getGzA(){
		return _gzA;
	}
	
	public void setA(String a){
		_a = a;
	}
	
	public String getA(){
		return _a;
	}
	
	public void setR(String r){
		_r = r;
	}
	
	public void setGzR(boolean gzr){
		_gzR = gzr;
	}
	
	public boolean getGzR(){
		return _gzR;
	}
	
	public String getR(){
		return _r;
	}
	
	public void setGzC(boolean gzc){
		_gzC = gzc;
	}
	
	public boolean getGzC(){
		return _gzC;
	}
	
	public void setC(String c){
		_c = c;
	}
	
	public String getC(){
		return _c;
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
	
	public void setStatsOut(String statsOut){
		_statsOut = statsOut;
	}
	
	public String getStatsOut(){
		return _statsOut;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
		_localGatherDir = getDefaultLocalGatherDir(outdir);
		_statsOut = getDefaultStatsFile(outdir);
	}
	
	public SlaveHoboStatsArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String scatterDir = RMIUtils.getLocalName(_scatterDir, server);
		String localGatherDir = RMIUtils.getLocalName(_localGatherDir, server);
		String statsOut = RMIUtils.getLocalName(_statsOut, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		String access = RMIUtils.getLocalName(_a, server);
		String redirs = RMIUtils.getLocalName(_r, server);
		String cons = RMIUtils.getLocalName(_c, server);
		
		SlaveHoboStatsArgs saa = new SlaveHoboStatsArgs(in, out);
		saa.setGzIn(_gzIn);
		saa.setGzA(_gzA);
		saa.setGzR(_gzR);
		saa.setGzC(_gzC);
		
		saa.setScatterDir(scatterDir);
		saa.setSlaveLog(log);
		saa.setTmpDir(tmpdir);
		saa.setLocalGatherDir(localGatherDir);
		saa.setStatsOut(statsOut);
		saa.setA(access);
		saa.setR(redirs);
		saa.setC(cons);
		
		//don't localise
		saa.setRemoteGatherDir(_remoteGatherDir);
		
		return saa;
	}
	
	public String toString(){
		return "in:"+_in+" gzIn:"+_gzIn+" a:"+_a+" gzA:"+_gzA+" r:"+_r+" gzR:"+_gzR+" c:"+_c+" gzC:"+_gzC+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir+" localGatherDir:"+_localGatherDir+" statsOut:"+_statsOut;
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
	
	public static final String getDefaultStatsFile(String outdir){
		return outdir+"/stats.jo.gz";
	}
}
