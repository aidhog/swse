package org.semanticweb.swse.saor.master;

import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.saor.RMIReasonerServer;
import org.semanticweb.swse.saor.SlaveReasonerArgs;

public class MasterReasonerArgs extends MasterArgs<MasterReasonerArgs>{
	public static final RMIReasonerServer DEFAULT_RI = new RMIReasonerServer();
	public static final MasterReasoner DEFAULT_MASTER = new MasterReasoner();
	
	public static final String DEFAULT_TBOX_FILENAME_GZ = "tbox.nq.gz";
	public static final String DEFAULT_TBOX_FILENAME_NGZ = "tbox.nq";
	
	public static final String DEFAULT_R_TBOX_FILENAME_GZ = "tbox.r.nq.gz";
	public static final String DEFAULT_R_TBOX_FILENAME_NGZ = "tbox.r.nq";
	
	private boolean _gzouttbox = DEFAULT_GZ_OUT;
	private String _outtbox = null;
	
	private boolean _gzoutrtbox = DEFAULT_GZ_OUT;
	private String _outrtbox = null;
	
//	private String _redirs = null;
//	private boolean _gzredirs = DEFAULT_GZ_IN;
	
	private SlaveReasonerArgs _sra = null;
	
	public MasterReasonerArgs(String outtbox, String outrtbox, SlaveReasonerArgs sra){
		_outrtbox = outrtbox;
		_outtbox = outtbox;
//		_redirs = lredirs;
		_sra = sra;
	}
	
	public void setGzTboxOut(boolean gzouttbox){
		_gzouttbox = gzouttbox;
	}
	
	public boolean getGzTboxOut(){
		return _gzouttbox;
	}
	
//	public void setGzRedirects(boolean gzredirs){
//		_gzredirs = gzredirs;
//	}
//	
//	public boolean getGzRedirects(){
//		return _gzredirs;
//	}
	
	public void setGzReasonedTboxOut(boolean gzoutrtbox){
		_gzoutrtbox = gzoutrtbox;
	}
	
	public boolean getGzReasonedTboxOut(){
		return _gzoutrtbox;
	}
	
	public String getTboxOut(){
		return _outtbox;
	}
	
	public String getReasonedTboxOut(){
		return _outrtbox;
	}
	
//	public String getRedirects(){
//		return _redirs;
//	}
	
	public Rule[] getRules(){
		return _sra.getRules();
	}
	
	public SlaveReasonerArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public String toString(){
		return "Master:: tboxout:"+_outtbox+" gztboxout:"+_gzouttbox+" tboxrout:"+_outrtbox+" gzrtboxout:"+_gzoutrtbox+"\n"
			+"Slave:: "+_sra;
	}
	
	public static final String getDefaultTboxOut(String outdir){
		return getDefaultTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultReasonedTboxOut(String outdir){
		return getDefaultReasonedTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultReasonedTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_R_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_R_TBOX_FILENAME_NGZ;
	}
	
	public RMIReasonerServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterReasoner getTaskMaster() {
		return DEFAULT_MASTER;
	}
}