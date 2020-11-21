package org.semanticweb.swse.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.nxindex.ScanIterator;
import org.semanticweb.nxindex.block.NodesBlockReader;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;

public class FilterRedirects {
	static transient Logger _log = Logger.getLogger(FilterRedirects.class.getName());
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws org.semanticweb.yars.nx.parser.ParseException, IOException {
		Options	options = new Options();
		
		org.semanticweb.yars.nx.cli.Main.addTicksOption(options);
		
		Option inputO = new Option("c", "name of sorted context file to read");
		inputO.setArgs(1);
		inputO.setRequired(true);
		options.addOption(inputO);
		
		Option inputfO = new Option("cf", "input format of context file, nx, nx.gz, nxz");
		inputfO.setArgs(1);
		options.addOption(inputfO);
		
		Option redirO = new Option("r", "name of file containing redirs");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option ranksfO = new Option("rf", "redirs format: nx, nx.gz, nxz");
		ranksfO.setArgs(1);
		options.addOption(ranksfO);
		
		Option outputO = new Option("o", "name of output redirects file");
		outputO.setArgs(1);
		outputO.setRequired(true);
		options.addOption(outputO);
		
		Option soutputO = new Option("so", "name of sorted output redirects file (if needed)");
		soutputO.setArgs(1);
		options.addOption(soutputO);

		Option ogzO = new Option("ogz", "flag to gzip output");
		ogzO.setArgs(0);
		options.addOption(ogzO);
		
		Option sogzO = new Option("sogz", "flag to gzip sorted output");
		sogzO.setArgs(0);
		options.addOption(sogzO);
		
		Option nO = new Option("n", "flag to normalise URIs (e.g., remove frag IDs)");
		nO.setArgs(0);
		options.addOption(nO);
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
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
		
		int[] pos = new int[]{0,1};
		if(cmd.hasOption("p"))
			pos = NodeComparatorArgs.getIntegerMask(cmd.getOptionValue("p"));
		
		int ticks = org.semanticweb.yars.nx.cli.Main.getTicks(cmd);
		
		boolean norm = cmd.hasOption("n");
		
		String redirs = cmd.getOptionValue("r");
		Iterator<Node[]> redirIter = null;
		NodesBlockReader redirNbr = null;
		InputStream redirIs = null;
		String flag = cmd.getOptionValue("rf");
		if(flag==null) flag = "nx";
			
		if(flag.equals("nx") || flag.equals("nx.gz")){
			if(redirs.equals("-")){
				redirIs = System.in;
			} else{
				redirIs = new FileInputStream(redirs);
			}

			if(flag.equals("nx.gz")){
				redirIs = new GZIPInputStream(redirIs);
			}

			redirIter = new NxParser(redirIs);
		} else if(flag.equals("nxz")){
			if(redirs.equals("-")){
				System.err.println("***ERROR: need filename for format nxz -- cannot read stdin");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("parameters:", options );
				return;
			}
			redirNbr = new NodesBlockReaderNIO(redirs);
			redirIter = new ScanIterator(redirNbr);
		} else{
			System.err.println("***ERROR: illegal value "+cmd.getOptionValue("rf")+" for 'rf'");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		String cons = cmd.getOptionValue("c");
		Iterator<Node[]> consIter = null;
		NodesBlockReader consNbr = null;
		InputStream consIs = null;
		String cflag = cmd.getOptionValue("cf");
		if(cflag==null) cflag = "nx";
			
		if(cflag.equals("nx") || cflag.equals("nx.gz")){
			if(cons.equals("-")){
				consIs = System.in;
			} else{
				consIs = new FileInputStream(cons);
			}

			if(flag.equals("nx.gz")){
				consIs = new GZIPInputStream(consIs);
			}

			consIter = new NxParser(consIs);
		} else if(flag.equals("nxz")){
			if(cons.equals("-")){
				System.err.println("***ERROR: need filename for format nxz -- cannot read stdin");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("parameters:", options );
				return;
			}
			consNbr = new NodesBlockReaderNIO(cons);
			consIter = new ScanIterator(consNbr);
		} else{
			System.err.println("***ERROR: illegal value "+cmd.getOptionValue("rf")+" for 'rf'");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		OutputStream os = new FileOutputStream(cmd.getOptionValue("o"));
		if(cmd.hasOption("ogz")){
			os = new GZIPOutputStream(os);
		}
		CallbackNxOutputStream cnos = new CallbackNxOutputStream(os);

		HashSet<Node> chs = new HashSet<Node>();
		
		_log.info("Loading contexts...");
		while(consIter.hasNext()){
			Node[] next = consIter.next();
			chs.add(next[0]);
		}
		_log.info("...loaded "+chs.size()+" contexts");
		
		_log.info("Scanning redirects...");
		int skip =0, read = 0;
		while(redirIter.hasNext()){
			Node[] next = redirIter.next();
			read++;
			
			if(norm) next = normalise(next);
			
			if(chs.contains(next[0]) || next[0].equals(next[1])){
				_log.info("Skipping "+Nodes.toN3(next));
				skip++;
			} else{
				cnos.processStatement(next);
			}
		}
		_log.info("...read "+read+" redirects; skipped "+skip);
		
		
		chs = null;
		os.close();
		
		if(cmd.hasOption("so")){
			_log.info("Sorting redirects...");
			OutputStream sos = new FileOutputStream(cmd.getOptionValue("so"));
			if(cmd.hasOption("sogz")){
				sos = new GZIPOutputStream(sos);
			}
			CallbackNxOutputStream csnos = new CallbackNxOutputStream(sos);
			
			InputStream is = new FileInputStream(cmd.getOptionValue("o"));
			if(cmd.hasOption("ogz")) is = new GZIPInputStream(is);
			
			NxParser nxp = new NxParser(is);
			
			SortIterator si = new SortIterator(nxp);
			
			Node[] old = null;
			while(si.hasNext()){
				Node[] next = si.next();
				
				if(old!=null && old[0].equals(next[0])){
					_log.severe("Dupe redirect for "+next[0]+" "+old[1]+" "+next[1]);
				}
				old = next;
				
				csnos.processStatement(next);
			}
			sos.close();
			is.close();
		}
		
		if(redirIs!=null)redirIs.close();
		if(redirNbr!=null)redirNbr.close();
		if(consIs!=null)consIs.close();
		if(consNbr!=null) consNbr.close();
	}

	private static Node[] normalise(Node[] next) {
		Node[] norm = new Node[next.length];
		for(int i=0; i<norm.length; i++){
			if(next[i] instanceof Resource){
				try{
					URI u = new URI(next[i].toString()); 
					u = LinkFilter.normalise(u);
					norm[i] = new Resource(u.toString());
				} catch(Exception e){
					norm[i] = next[i];
				}
			} else{
				norm[i] = next[i];
			}
		}
		return norm;
	}	
}