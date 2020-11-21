package org.semanticweb.swse.econs.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.ercons.RMIConsolidationServer;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator.HandleNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunLocalSameAsConsolidation {
	private final static Logger _log = Logger.getLogger(RunLocalSameAsConsolidation.class.getSimpleName());

	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();

		Option in1O = new Option("i1", "gz nx sorted data to consolidate");
		in1O.setArgs(1);
		in1O.setRequired(true);
		options.addOption(in1O);
		
		Option in2O = new Option("i2", "gz nx sorted closed sameas file s1>s2");
		in2O.setArgs(1);
		in2O.setRequired(true);
		options.addOption(in2O);

		Option outO = new Option("o", "output dir");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option helpO = new Option("h", "print help");
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		InputStream is = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i1")));
		Iterator<Node[]> nxp = new NxParser(is);
		
		Iterator<Node[]> sao = new FilterPredIterator(nxp, OWL.SAMEAS);
		
		SortIterator si = new SortIterator(sao);
		
		InputStream sais = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i2")));
		Iterator<Node[]> sanxp = new NxParser(sais);

		String outputDir = cmd.getOptionValue("o");
		RMIUtils.mkdirs(outputDir);
		String consS = outputDir+"/"+"cons.sub.sa.nx.gz";
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(consS));
		Callback cb = new CallbackNxOutputStream(os);
		
		long b4 = System.currentTimeMillis();
		
		int pos = 0;
		RMIConsolidationServer.rewrite(si, sanxp, cb, pos, HandleNode.FILTER, HandleNode.REWRITE);
		
		is.close();
		os.close();
		sais.close();
		_log.info("...finished subject consolidation in "+(System.currentTimeMillis()-b4)+" ms.");
		
		
		is = new GZIPInputStream(new FileInputStream(consS));
		nxp = new NxParser(is);
		
		SortArgs sa = new SortArgs(nxp);
		sa.setComparator(new NodeComparator(new int[]{2,1,0,3,4}));
		si = new SortIterator(sa);
		
		sais = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i2")));
		sanxp = new NxParser(sais);
		
		String consO = outputDir+"/"+"cons.obj.sa.nx.gz";
		os = new GZIPOutputStream(new FileOutputStream(consO));
		cb = new CallbackNxOutputStream(os);
		
		pos = 2;
		RMIConsolidationServer.rewrite(si, sanxp, cb, pos, HandleNode.FILTER, HandleNode.REWRITE);
		
		is.close();
		os.close();
		sais.close();
		
		is = new GZIPInputStream(new FileInputStream(consO));
		nxp = new NxParser(is);
		
		sa = new SortArgs(nxp);
		sa.setComparator(new NodeComparator(new int[]{2,1,0,3,4,5}));
		si = new SortIterator(sa);
		
		String consSO = outputDir+"/"+"cons.obj.sa.s.nx.gz";
		os = new GZIPOutputStream(new FileOutputStream(consSO));
		cb = new CallbackNxOutputStream(os);
		
		while(si.hasNext()){
			cb.processStatement(si.next());
		}
		
		is.close();
		os.close();
		sais.close();
		
		is = new GZIPInputStream(new FileInputStream(consO));
		nxp = new NxParser(is);
		
		sa = new SortArgs(nxp);
		si = new SortIterator(sa);
		
		String consSS = outputDir+"/"+"cons.sub.sa.s.nx.gz";
		os = new GZIPOutputStream(new FileOutputStream(consSS));
		cb = new CallbackNxOutputStream(os);
		
		while(si.hasNext()){
			cb.processStatement(si.next());
		}
		
		is.close();
		os.close();
		sais.close();
	}
	
	public static class FilterPredIterator implements Iterator<Node[]>{
		Iterator<Node[]> _in;
		Node[] _current = null;
		Set<Node> _preds;
		
		public FilterPredIterator(Iterator<Node[]> in, Node pred){
			_in = in;
			_preds = new HashSet<Node>();
			_preds.add(pred);
			loadNext();
		}
		
		public FilterPredIterator(Iterator<Node[]> in, HashSet<Node> preds){
			_in = in;
			_preds = preds;
			loadNext();
		}
		
		private void loadNext(){
			_current = null;
			while(_in.hasNext()){
				Node[] next = _in.next();
				if(_preds.contains(next[1])){
					_current = next;
					return;
				}
			}
		}
		
		public boolean hasNext() {
			return _current!=null;
		}

		public Node[] next() {
			Node[] last = _current;
			loadNext();
			return last;
		}

		public void remove() {
			_in.remove();
		}
		
	}
}
