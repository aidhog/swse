package org.semanticweb.swse.tasks;

import java.util.ArrayList;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.SlaveConsolidationArgs;
import org.semanticweb.swse.cons.master.MasterConsolidationArgs;
import org.semanticweb.swse.index.SlaveIndexerArgs;
import org.semanticweb.swse.index.master.MasterIndexerArgs;
import org.semanticweb.swse.lucene.SlaveLuceneArgs;
import org.semanticweb.swse.lucene.master.MasterLuceneArgs;
import org.semanticweb.swse.rank.SlaveRankingArgs;
import org.semanticweb.swse.rank.master.MasterRankingArgs;
import org.semanticweb.swse.saor.SlaveReasonerArgs;
import org.semanticweb.swse.saor.master.MasterReasonerArgs;

public abstract class Tasks {
	
	public static final String RANK_U_DIR = "ranku/";
	public static final String CONS_DIR = "cons/";
	public static final String RANK_C_DIR = "rankc/";
	public static final String REASON_DIR = "reason/";
	public static final String INDEX_DIR = "index/";
	public static final String LUCENE_DIR = INDEX_DIR+"lucene/";
	
	public static final String RANK_U_SLOG = RANK_U_DIR+"rank.s.log";
	public static final String CONS_SLOG = CONS_DIR+"cons.s.log";
	public static final String RANK_C_SLOG = RANK_C_DIR+"rankc.s.log";
	public static final String REASON_SLOG = REASON_DIR+"reason.s.log";
	public static final String INDEX_SLOG = INDEX_DIR+"index.s.log";
	public static final String LUCENE_SLOG = LUCENE_DIR+"lucene.s.log";
	
	public static final String RANK_U_MLOG = RANK_U_DIR+"rank.m.log";
	public static final String CONS_MLOG = CONS_DIR+"cons.m.log";
	public static final String RANK_C_MLOG = RANK_C_DIR+"rankc.m.log";
	public static final String REASON_MLOG = REASON_DIR+"reason.m.log";
	public static final String INDEX_MLOG = INDEX_DIR+"index.m.log";
	public static final String LUCENE_MLOG = LUCENE_DIR+"lucene.m.log";
	
	public static MasterArgs[] createTask(String in, boolean gzin, String lredirs, boolean gzlredirs, String rredirs, boolean gzrredirs, String outdir){
		ArrayList<MasterArgs<?>> tasks = new ArrayList<MasterArgs<?>>();
		
		//RANK UNCONSOLIDATED
		String outdirru = outdir+"/"+RANK_U_DIR;
		SlaveRankingArgs sra1 = new SlaveRankingArgs(in, in, rredirs, outdirru);
		sra1.setGzRedirects(gzrredirs);
		sra1.setGzInId(gzin);
		sra1.setGzInNa(gzin);
		sra1.setOutFinal(SlaveRankingArgs.getDefaultRanksOut(outdirru));
		sra1.setSlaveLog(outdir+"/"+RANK_U_SLOG);
		
		MasterRankingArgs mra1 = new MasterRankingArgs(
				MasterRankingArgs.getDefaultRanksOut(RMIUtils.getLocalName(outdirru))
				, sra1);
		mra1.setMasterLog(RMIUtils.getLocalName(outdir+"/"+RANK_U_MLOG));
		
		tasks.add(mra1);
		
		//CONSOLIDATE
		String outdircons = outdir+"/"+CONS_DIR;
		SlaveConsolidationArgs sca1 = new SlaveConsolidationArgs(in,
				SlaveConsolidationArgs.getDefaultSameasOut(outdircons),
						SlaveConsolidationArgs.getDefaultOut(outdircons)
										);
		sca1.setGzIn(gzin);
		sca1.setSlaveLog(outdir+"/"+CONS_SLOG);

		MasterConsolidationArgs mca1 = new MasterConsolidationArgs(MasterConsolidationArgs.getDefaultSameasOut(RMIUtils.getLocalName(outdir)), sca1);
		mca1.setMasterLog(RMIUtils.getLocalName(outdir+"/"+CONS_MLOG));
		mca1.setRanks(RMIUtils.getLocalName(sra1.getOutFinal()), mra1.getGzRanksOut());
		
		tasks.add(mca1);
		
		//RANK CONSOLIDATED
		String outdirrc = outdir+"/"+RANK_C_DIR;
		SlaveRankingArgs sra2 = new SlaveRankingArgs(in, sca1.getOut(), rredirs, outdirrc);
		sra2.setGzRedirects(gzrredirs);
		sra2.setGzInId(sca1.getGzOut());
		sra2.setGzInNa(gzin);
		sra2.setOutFinal(SlaveRankingArgs.getDefaultRanksOut(outdirrc));
		sra2.setSlaveLog(outdir+"/"+RANK_C_SLOG);
		
		MasterRankingArgs mra2 = new MasterRankingArgs(MasterRankingArgs.getDefaultRanksOut(RMIUtils.getLocalName(outdirru))
				, sra2);
		mra2.setMasterLog(RMIUtils.getLocalName(outdir+"/"+RANK_C_MLOG));
		
		tasks.add(mra2);
		
		//REASON
		String outdirreas= outdir+"/"+REASON_DIR;
		SlaveReasonerArgs sra3 = new SlaveReasonerArgs(in, sca1.getOut(), rredirs, 
				SlaveReasonerArgs.getDefaultTboxOut(outdirreas),
				SlaveReasonerArgs.getDefaultReasonedOut(outdirreas));
		sra3.setGzInTbox(gzin);
		sra3.setGzInAbox(sca1.getGzOut());
		sra3.setGzRedirects(gzrredirs);
		sra3.setSlaveLog(outdir+"/"+REASON_SLOG);

		MasterReasonerArgs mra3 = new MasterReasonerArgs(
				MasterReasonerArgs.getDefaultTboxOut(RMIUtils.getLocalName(outdirreas)),
				MasterReasonerArgs.getDefaultReasonedTboxOut(RMIUtils.getLocalName(outdirreas)),
				sra3);
		mra3.setMasterLog(RMIUtils.getLocalName(outdir+"/"+REASON_MLOG));
		
		tasks.add(mra3);
		
		//INDEX
		String outdiridx = outdir+"/"+INDEX_DIR;
		
		SlaveIndexerArgs sia = new SlaveIndexerArgs(SlaveIndexerArgs.getDefaultScatterDir(outdiridx),
				SlaveIndexerArgs.getDefaultGatherDir(outdiridx),
				SlaveIndexerArgs.getDefaultIndexFile(outdiridx),
				SlaveIndexerArgs.getDefaultSparseFile(outdiridx)
				);
		sia.setSlaveLog(outdir+"/"+INDEX_SLOG);
		
		String[] localfiles = new String[]{ mra3.getReasonedTboxOut() };
		boolean[] localgzip = new boolean[]{ mra3.getGzReasonedTboxOut() };
		
		String[] remotefiles = new String[]{ sca1.getOut(), sra3.getOutReasoned() };
		boolean[] remotegzip = new boolean[]{ sca1.getGzOut(), sra3.getGzOutReasoned() };
		
		MasterIndexerArgs mia = new MasterIndexerArgs(localfiles, remotefiles, 
				MasterIndexerArgs.getDefaultScatterDir(RMIUtils.getLocalName(outdiridx)), 
				sia);
		mia.setGzLocal(localgzip);
		mia.setGzRemote(remotegzip);
		mia.setMasterLog(RMIUtils.getLocalName(outdir+"/"+INDEX_MLOG));
		
		tasks.add(mia);
		
		//LUCENE
		String outdirluc = outdir+"/"+LUCENE_DIR;
		
		SlaveLuceneArgs sla = new SlaveLuceneArgs(sia.getOutIndex(), sra2.getOutFinal(), rredirs, outdirluc);
		sla.setGzRanks(mra2.getGzRanksOut());
		sla.setGzRedirects(gzrredirs);
		sla.setSlaveLog(outdir+"/"+LUCENE_SLOG);
		
		MasterLuceneArgs mla = new MasterLuceneArgs(sla);
		mla.setMasterLog(RMIUtils.getLocalName(outdir+"/"+LUCENE_MLOG));
		
		tasks.add(mla);
		
		MasterArgs[] tasksA = new MasterArgs[tasks.size()];
		tasks.toArray(tasksA);
		
		return tasksA;
	}
}
