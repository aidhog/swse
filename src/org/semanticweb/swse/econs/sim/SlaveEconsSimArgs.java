package org.semanticweb.swse.econs.sim;

import java.util.ArrayList;
import java.util.HashMap;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.yars.nx.Node;

public class SlaveEconsSimArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final String DEFAULT_TMP_DIR = "tmp/";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.final.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_OUT_RAW_FILENAME_NGZ = "sim.sp.nx";
	public static final String DEFAULT_OUT_RAW_FILENAME_GZ = DEFAULT_OUT_RAW_FILENAME_NGZ+".gz";
	
//	public static final String DEFAULT_OUT_RAW_OP_FILENAME_NGZ = "sim.op.nx.gz";
//	public static final String DEFAULT_OUT_RAW_OP_FILENAME_GZ = DEFAULT_OUT_RAW_OP_FILENAME_NGZ+".gz";
	
//	public static final int DEFAULT_BUFFER_SIZE = 50000;
	
	public static final int DEFAULT_LIMIT = 38;
	
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather%/";
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TMP_DIR+"scatter/";
	public static final String DEFAULT_LOCAL_GATHER_DIR = "gall/";
	
	public static final int TOP_K = 10;
	public static final int RAND_K = 100;
	
	private String _inSpoc;
	private boolean _gzInSpoc = MasterArgs.DEFAULT_GZ_IN;
	
	private String _inOpsc;
	private boolean _gzInOpsc = MasterArgs.DEFAULT_GZ_IN;
	
	private String _outData;
	private boolean _gzOutData = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outRaw;
	private boolean _gzOutRaw = MasterArgs.DEFAULT_GZ_OUT;
	
//	private String _outRawOp;
//	private boolean _gzOutRawOp = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
//	private int _buf = DEFAULT_BUFFER_SIZE; 
	
	private int _limit = DEFAULT_LIMIT; 
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _localGatherDir;
	
	private ArrayList<HashMap<Node,PredStats>> _predStats = null;
	
	public SlaveEconsSimArgs(String inSpoc, String inOpsc, String outdir){
		_inSpoc = inSpoc;
		_inOpsc = inOpsc;
		_outdir = outdir;
		initDefaults(outdir);
	}
	
	public String getOutDir(){
		return _outdir;
	}
	
	public String getOutData(){
		return _outData;
	}
	
	public String getInOpsc(){
		return _inOpsc;
	}
	
	public void setGzInOpsc(boolean gzin){
		_gzInOpsc = gzin;
	}
	
	public void setPredStats(ArrayList<HashMap<Node,PredStats>> predStats){
		_predStats = predStats;
	}
	
	public ArrayList<HashMap<Node,PredStats>> getPredStats(){
		return _predStats;
	}
	
//	public int getBufSize(){
//		return _buf;
//	}
//	
//	public void setBufSize(int buf){
//		_buf = buf;
//	}
	
	public int getLimit(){
		return _limit;
	}
	
	public void setLimit(int limit){
		_limit = limit;
	}
	
	public boolean getGzInOpsc(){
		return _gzInOpsc;
	}
	
	public String getInSpoc(){
		return _inSpoc;
	}
	
	public void setGzInSpoc(boolean gzin){
		_gzInSpoc = gzin;
	}
	
	public boolean getGzInSpoc(){
		return _gzInSpoc;
	}
	
	public void setTmpDir(String tmpDir){
		_tmpdir = tmpDir;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public String getRawOut(){
		return _outRaw;
	}
	
	public void setGzRawOut(boolean gzro){
		_gzOutRaw = gzro;
	}
	
	public boolean getGzRawOut(){
		return _gzOutRaw;
	}
	
//	public String getRawOutOp(){
//		return _outRawOp;
//	}
//	
//	public void setGzRawOutOp(boolean gzro){
//		_gzOutRawOp = gzro;
//	}
//	
//	public boolean getGzRawOutOp(){
//		return _gzOutRawOp;
//	}
	
	public void setGzData(boolean gzdata){
		_gzOutData = gzdata;
	}
	
	public boolean getGzData(){
		return _gzOutData;
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
		_outData = getDefaultOutFinalFilename(outdir);
		_outRaw = getDefaultOutRaw(outdir);
//		_outRawOp = getDefaultOutRawOp(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
		_localGatherDir = getDefaultLocalGatherDir(outdir);
	}
	
	public SlaveEconsSimArgs instantiate(int server) {
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		String outsp = RMIUtils.getLocalName(_outRaw, server);
//		String outop = RMIUtils.getLocalName(_outRawOp, server);
		String outdata = RMIUtils.getLocalName(_outData, server);
		String gDir = RMIUtils.getLocalName(_localGatherDir, server);
		String sDir = RMIUtils.getLocalName(_scatterDir, server);
		
		String inOpsc = null, inSpoc = null;
		if(_inOpsc!=null){
			inOpsc = RMIUtils.getLocalName(_inOpsc, server);
		} 
		if(_inSpoc!=null){
			inSpoc = RMIUtils.getLocalName(_inSpoc, server);
		}
		
		SlaveEconsSimArgs saa = new SlaveEconsSimArgs(inSpoc, inOpsc, out);
		saa._outData = outdata;
		saa._outRaw = outsp;
//		saa._outRawOp = outop;
		saa.setGzData(_gzOutData);
		saa.setGzInOpsc(_gzInOpsc);
		saa.setGzInSpoc(_gzInSpoc);
		saa.setGzRawOut(_gzOutRaw);
//		saa.setGzRawOutOp(_gzOutRawOp);
		saa.setSlaveLog(log);
		saa.setTmpDir(tmpdir);
		saa.setRemoteGatherDir(_remoteGatherDir);
		saa.setLocalGatherDir(gDir);
		saa.setScatterDir(sDir);
		saa.setPredStats(_predStats);
		
		return saa;
	}
	
	public String toString(){
		return "inS:"+_inSpoc+" gzInS:"+_gzInSpoc+"inO:"+_inOpsc+" gzInO:"+_gzInOpsc+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" outRaw:"+_outRaw+" rawgz:"+_gzOutRaw+" outdata:"+_outData+" outdatagz:"+_gzOutData
		+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir+" localGatherDir:"+_localGatherDir;
	}
	
	public static final String getDefaultOutFinalFilename(String outdir){
		return getDefaultOutFinalFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutFinalFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_NGZ;
	}
	
	public static final String getDefaultOutRaw(String outdir){
		return getDefaultOutRaw(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutRaw(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_RAW_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_RAW_FILENAME_NGZ;
	}
	
//	public static final String getDefaultOutRawOp(String outdir){
//		return getDefaultOutRawOp(outdir, MasterArgs.DEFAULT_GZ_OUT);
//	}
//	
//	public static final String getDefaultOutRawOp(String outdir, boolean gz){
//		if(gz)
//			return outdir+"/"+DEFAULT_OUT_RAW_OP_FILENAME_GZ;
//		return outdir+"/"+DEFAULT_OUT_RAW_OP_FILENAME_GZ;
//	}
	
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
