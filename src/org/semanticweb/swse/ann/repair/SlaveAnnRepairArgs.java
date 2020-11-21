package org.semanticweb.swse.ann.repair;

import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveAnnRepairArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8759673213202538048L;

	public static final String DEFAULT_REPAIRED_FILENAME_GZ = "data.repaired.nx.gz";
	
	final String _inRaw, _inFinal, _inTbox;
	
	final String _outFinal;
	
	public SlaveAnnRepairArgs(String inRaw, String inFinal, String inTbox, String outRepair){
		_inRaw = inRaw;
		_inFinal = inFinal;
		_outFinal = outRepair;
		_inTbox = inTbox;
	}
	
	
	public SlaveAnnRepairArgs instantiate(int server) {
		String inRaw = RMIUtils.getLocalName(_inRaw, server);
		String inFinal = RMIUtils.getLocalName(_inFinal, server);
		String outFinal = RMIUtils.getLocalName(_outFinal, server);
		String inTbox = RMIUtils.getLocalName(_inTbox, server);
		
		SlaveAnnRepairArgs sca = new SlaveAnnRepairArgs(inRaw, inFinal, inTbox, outFinal);
		
		return sca;
	}
	
	public String toString(){
		return "inRaw:"+_inRaw+" inFinal:"+_inFinal+" inTbox:"+_inTbox+" outFinal:"+_outFinal;
	}
	
	public static final String getDefaultRepairedOut(String outdir){
		return outdir+"/"+DEFAULT_REPAIRED_FILENAME_GZ;
	}
}
