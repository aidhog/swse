package org.semanticweb.swse.rank.master;

import org.deri.idrank.RankGraph;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.rank.RMIRankingServer;
import org.semanticweb.swse.rank.SlaveRankingArgs;

public class MasterRankingArgs extends MasterArgs<MasterRankingArgs>{
	public static final RMIRankingServer DEFAULT_RI = new RMIRankingServer();
	public static final MasterRanker DEFAULT_MASTER = new MasterRanker();
	
	public static final String DEFAULT_RANKS_FILENAME_GZ = "ranks.nx.gz";
	public static final String DEFAULT_RANKS_FILENAME_NGZ = "ranks.nx";
	
	private boolean _gzoutranks = DEFAULT_GZ_OUT;
	private String _outranks = null;
	private SlaveRankingArgs _sra = null;
	private int _iters = RankGraph.ITERATIONS;
	private float _damping = RankGraph.DAMPING;
	
	public MasterRankingArgs(String outranks, SlaveRankingArgs sra){
		_outranks = outranks;
		_sra = sra;
	}
	
	public void setGzRanksOut(boolean gzoutranks){
		_gzoutranks = gzoutranks;
	}
	
	public boolean getGzRanksOut(){
		return _gzoutranks;
	}
	
	public String getLocalRanksOut(){
		return _outranks;
	}
	
	public void setIterations(int iters){
		_iters = iters;
	}
	
	public int getIterations(){
		return _iters;
	}
	
	public void setDamping(float damping){
		_damping = damping;
	}
	
	public float getDamping(){
		return _damping;
	}
	
	public SlaveRankingArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public String toString(){
		return "Master:: outranks:"+_outranks+" gzranks:"+_gzoutranks+" iters:"+_iters+" damp:"+_damping+"\n"
			+"Slave:: "+_sra;
	}
	
	public static final String getDefaultRanksOut(String outdir){
		return getDefaultRanksOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultRanksOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_RANKS_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_RANKS_FILENAME_NGZ;
	}
	
	public RMIRankingServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterRanker getTaskMaster() {
		return DEFAULT_MASTER;
	}
}