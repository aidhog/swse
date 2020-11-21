package org.semanticweb.swse.cons;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveConsolidationArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2249721834721595373L;

	public static final String DEFAULT_CONS_FILENAME_GZ = "data.cons.nq.gz";
	public static final String DEFAULT_CONS_FILENAME_NGZ = "data.cons.nq";
	
	public static final String DEFAULT_SAMEAS_FILENAME = "sameas.nx.gz";
	
	private String _in;
	private boolean _gzin = MasterArgs.DEFAULT_GZ_IN;
	
	private String _saout = null;
	
	private String _out;
	private boolean _gzout = MasterArgs.DEFAULT_GZ_OUT;
	
	public SlaveConsolidationArgs(String in, String saout, String out){
		_in = in;
		_out = out;
		_saout = saout;
	}
	
	public String getOut(){
		return _out;
	}
	
	public String getSameAsOut(){
		return _saout;
	}
	
	public String getIn(){
		return _in;
	}
	
	public void setGzIn(boolean gzin){
		_gzin = gzin;
	}
	
	public boolean getGzIn(){
		return _gzin;
	}
	
	public void setGzOut(boolean gzout){
		_gzout = gzout;
	}
	
	public boolean getGzOut(){
		return _gzout;
	}
	
//	public void setGzSameasOut(boolean gzsa){
//		_gzsa = gzsa;
//	}
	
//	public boolean getGzSameasOut(){
//		return _gzsa;
//	}
	
	

	public SlaveConsolidationArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String out = RMIUtils.getLocalName(_out, server);
		String saout = RMIUtils.getLocalName(_saout, server);
		
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveConsolidationArgs sca = new SlaveConsolidationArgs(in, saout, out);
		
		sca.setGzIn(_gzin);
		sca.setGzOut(_gzout);
		sca.setSlaveLog(log);
		
		return sca;
	}
	
	public String toString(){
		return "in:"+_in+" gzin:"+_gzin+" saout:"+_saout+" out:"+_out+" gzout:"+_gzout;
	}
	
	public static final String getDefaultOut(String outdir){
		return getDefaultOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_CONS_FILENAME_GZ;
		return outdir+"/"+DEFAULT_CONS_FILENAME_NGZ;
	}
	
	public static final String getDefaultSameasOut(String outdir){
		return outdir+"/"+DEFAULT_SAMEAS_FILENAME;
	}
}
