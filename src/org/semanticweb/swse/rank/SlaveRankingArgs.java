package org.semanticweb.swse.rank;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;
import org.semanticweb.swse.rank.master.MasterRankingArgs;

public class SlaveRankingArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final boolean DEFAULT_PLD = true;
	
	public static final boolean DEFAULT_TBOX_ID = false;
	public static final boolean DEFAULT_TBOX_NA = false;
	
	private String _inId;
	private boolean _gzinId = MasterArgs.DEFAULT_GZ_IN;
	
	private String _inNa;
	private boolean _gzinNa = MasterArgs.DEFAULT_GZ_IN;
	
	private boolean _pld = DEFAULT_PLD;
	
	private boolean _tboxId = DEFAULT_TBOX_ID;
	private boolean _tboxNa = DEFAULT_TBOX_NA;
	
	private String _out;
	
	private String _outFinal = null;
	
	private String _r;
	private boolean _gzR = MasterArgs.DEFAULT_GZ_IN;
	
	public SlaveRankingArgs(String inna, String inid, String redirects, String outdir){
		_inNa = inna;
		_inId = inid;
		_out = outdir;
		_r = redirects;
	}
	
	public void setOutFinal(String outFinal){
		_outFinal = outFinal;
	}
	
	public String getOutFinal(){
		return _outFinal;
	}
	
	public String getRedirects(){
		return _r;
	}
	
	public String getOut(){
		return _out;
	}
	
	public String getInNa(){
		return _inNa;
	}
	
	public String getInId(){
		return _inId;
	}
	
	public void setGzInId(boolean gzinid){
		_gzinId = gzinid;
	}
	
	public boolean getGzInId(){
		return _gzinId;
	}
	
	public void setTboxId(boolean tboxid){
		_tboxId = tboxid;
	}
	
	public boolean getTboxId(){
		return _tboxId;
	}
	
	public void setTboxNa(boolean tboxna){
		_tboxNa = tboxna;
	}
	
	public boolean getTboxNa(){
		return _tboxNa;
	}
	
	public void setPLD(boolean pld){
		_pld = pld;
	}
	
	public boolean getPLD(){
		return _pld;
	}
	
	public void setGzInNa(boolean gzinna){
		_gzinNa = gzinna;
	}
	
	public boolean getGzInNa(){
		return _gzinNa;
	}
	
	public void setGzRedirects(boolean gzr){
		_gzR = gzr;
	}
	
	public boolean getGzRedirects(){
		return _gzR;
	}
	
	public SlaveRankingArgs instantiate(int server) {
		String inna = RMIUtils.getLocalName(_inNa, server);
		String inid = RMIUtils.getLocalName(_inId, server);
		String out = RMIUtils.getLocalName(_out, server);
		String r = RMIUtils.getLocalName(_r, server);
		
		String log = RMIUtils.getLocalName(_logFile, server);
		
		String outFinal = null;
		if(_outFinal!=null)
			outFinal = RMIUtils.getLocalName(_outFinal, server);
		
		SlaveRankingArgs sca = new SlaveRankingArgs(inna, inid, r, out);
		
		sca.setOutFinal(outFinal);
		sca.setSlaveLog(log);
		sca.setPLD(_pld);
		sca.setTboxId(_tboxId);
		sca.setTboxNa(_tboxNa);
		sca.setGzInId(_gzinId);
		sca.setGzInNa(_gzinNa);
		sca.setGzRedirects(_gzR);

		return sca;
	}
	
	public String toString(){
		return "inNa:"+_inNa+" gzinNa:"+_gzinNa+" inid:"+_inId+" gzinId:"+_gzinId+" pld:"+_pld+" tboxId:"+_tboxId+" tboxNa:"+_tboxNa+" out:"+_out+" redirs:"+_r+" gzredirs:"+_gzR+" [outFinal]:"+_outFinal;
	}
	
	public static final String getDefaultRanksOut(String outdir){
		return getDefaultRanksOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultRanksOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+MasterRankingArgs.DEFAULT_RANKS_FILENAME_GZ;
		return outdir+"/"+MasterRankingArgs.DEFAULT_RANKS_FILENAME_NGZ;
	}
}
