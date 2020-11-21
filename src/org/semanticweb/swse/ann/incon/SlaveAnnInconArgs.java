package org.semanticweb.swse.ann.incon;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveAnnInconArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8759673213202538048L;

	public static final boolean DEFAULT_HANDLE_COLLECTIONS = true;
	
	private String _in;
	private boolean _gzIn = MasterArgs.DEFAULT_GZ_IN;
	
	private String _outData;
	private boolean _gzOutData = MasterArgs.DEFAULT_GZ_OUT;
	
//	private String _outSmall;
//	private boolean _gzOutSmall = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outIc;
	private boolean _gzOutIc = MasterArgs.DEFAULT_GZ_OUT;
	
	public static final String DEFAULT_BUFFER_FILENAME_NGZ = "tbox.new.nq";
	public static final String DEFAULT_BUFFER_FILENAME_GZ = DEFAULT_BUFFER_FILENAME_NGZ+".gz";
	
//	public static final String DEFAULT_SMALL_FILENAME_NGZ = "tbox.nq";
//	public static final String DEFAULT_SMALL_FILENAME_GZ = DEFAULT_SMALL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_INCONSISTENCIES_FILENAME_NGZ = "inconsist.dat";
	public static final String DEFAULT_INCONSISTENCIES_FILENAME_GZ = DEFAULT_INCONSISTENCIES_FILENAME_NGZ+".gz";

	
	public SlaveAnnInconArgs(String in, String outInconData, String outIncons){
		_outData = outInconData;
		_outIc = outIncons;
//		_outSmall = outSmall;
		_in = in;
	}
	
	public String getOutData(){
		return _outData;
	}
	
//	public String getOutSmall(){
//		return _outSmall;
//	}
	
	public String getOutInconsistencies(){
		return _outIc;
	}
	
	public String getInput(){
		return _in;
	}
	
	public void setGzIn(boolean gzin){
		_gzIn = gzin;
	}
	
	public boolean getGzIn(){
		return _gzIn;
	}
	
//	public void setGzSmall(boolean gzsmall){
//		_gzOutSmall = gzsmall;
//	}
//	
//	public boolean getGzSmall(){
//		return _gzOutSmall;
//	}
	
	public void setGzInconsistencies(boolean gzic){
		_gzOutIc = gzic;
	}
	
	public boolean getGzInconsistencies(){
		return _gzOutIc;
	}
	
	public void setGzData(boolean gzdata){
		_gzOutData = gzdata;
	}
	
	public boolean getGzData(){
		return _gzOutData;
	}
	
	public SlaveAnnInconArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String outdata = RMIUtils.getLocalName(_outData, server);
		String outinc = RMIUtils.getLocalName(_outIc, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveAnnInconArgs sia = new SlaveAnnInconArgs(in, outdata, outinc);
		
		sia.setSlaveLog(log);
		sia.setGzIn(_gzIn);
		sia.setGzInconsistencies(_gzOutIc);
		sia.setGzData(_gzOutData);

		return sia;
	}
	
	public String toString(){
		return "in:"+_in+" gzin:"+_gzIn+" outdata:"+_outData+" gzoutdata:"+_gzOutData+" outincon:"+_outIc+" gzoutincon:"+_gzOutIc;
//		return "in:"+_in+" gzin:"+_gzIn+" outdata:"+_outData+" gzoutdata:"+_gzOutData+" outsmall:"+_outSmall+" gzoutsmall:"+_gzOutSmall+" outincon:"+_outIc+" gzoutincon:"+_gzOutIc;
	}
	
	public static final String getDefaultDataOut(String outdir){
		return getDefaultDataOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultDataOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_BUFFER_FILENAME_GZ;
		return outdir+"/"+DEFAULT_BUFFER_FILENAME_NGZ;
	}
	
	public static final String getDefaultInconsistenciesOut(String outdir){
		return getDefaultInconsistenciesOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultInconsistenciesOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_INCONSISTENCIES_FILENAME_GZ;
		return outdir+"/"+DEFAULT_INCONSISTENCIES_FILENAME_NGZ;
	}
}
