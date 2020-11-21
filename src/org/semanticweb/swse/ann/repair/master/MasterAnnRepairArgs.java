package org.semanticweb.swse.ann.repair.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerServer;
import org.semanticweb.swse.ann.reason.SlaveAnnReasonerArgs;
import org.semanticweb.swse.ann.repair.SlaveAnnRepairArgs;

public class MasterAnnRepairArgs extends MasterArgs<MasterAnnRepairArgs>{
	public static final RMIAnnReasonerServer DEFAULT_RI = new RMIAnnReasonerServer();
	public static final MasterAnnRepair DEFAULT_MASTER = new MasterAnnRepair();
	
	public static final boolean DEFAULT_AUTH = true;
	
	boolean _auth = DEFAULT_AUTH;
	
	final String _intbox;
	final String _ininc;
	
	boolean _inconGz = true;
	
	final SlaveAnnRepairArgs _sra;
	
	public MasterAnnRepairArgs(String intbox, String incons, SlaveAnnRepairArgs sra){
		_intbox = intbox;
		_ininc = incons;
		_sra = sra;
	}
	
	public void setGzIncon(boolean gz){
		_inconGz = true;
	}
	
	public SlaveAnnRepairArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public String toString(){
		return "Master:: intbox:"+_intbox+" inincon:"+_ininc+"\n"
			+"Slave:: "+_sra;
	}
	
	public RMIAnnReasonerServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterAnnRepair getTaskMaster() {
		return DEFAULT_MASTER;
	}
}