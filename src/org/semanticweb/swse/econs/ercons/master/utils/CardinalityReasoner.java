package org.semanticweb.swse.econs.ercons.master.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Class for performing Reasoning over FunctionalProperty 
 * instances using an on-disk index
 * @author aidhog
 */
public class CardinalityReasoner extends OnDiskReasoner {
	public static final String PREFIX = "card";
	private boolean _subjLits = false;
	private Map<Node,? extends Set<Node>> _cards;

	/**
	 * 
	 * @param dir - Directory for join indices for each iteration
	 * @param sai - SameAsIndex to be populated by reasoner and to be used for 
	 * rewriting input
	 * @throws IOException
	 */
	public CardinalityReasoner(String dir, HashMap<Node,? extends Set<Node>> cards, boolean considerSubjLiterals) throws IOException{
		super(dir, PREFIX);
		_cards = cards;
		_subjLits = considerSubjLiterals;
	}

	/**
	 * Perform iteration on statements added through use of addStatement method
	 * @return null
	 * @throws IOException
	 * @throws ParseException
	 */
	public Iterator<Node[]> performReasoning(Iterator<Node[]> sai) throws IOException, ParseException{
		_round++;
		_addedRound=0;

		Iterator<Node[]>in = prepareIndex(sai);
		return new CardIterator(in, _cards);
	}

	public class CardIterator implements Iterator<Node[]> {
		private Iterator<Node[]> _current;
		private Iterator<Node[]> _in;
		private Node[] _lastRead = null;
		private Map<Node,? extends Set<Node>> _cards;

		public CardIterator(Iterator<Node[]> in, Map<Node,? extends Set<Node>> cards){
			_in = in;
			_cards = cards;
			getNextBatch();
		}

		public boolean hasNext() {
			return _current!=null && _current.hasNext();
		}

		public Node[] next() {
			Node[] current = _current.next();
			if(!_current.hasNext()){
				getNextBatch();
			}
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public void getNextBatch(){
			if(!_in.hasNext())
				return;

			Node oldsubj = null;

			HashMap<Node,TreeSet<Node>> edges = new HashMap<Node,TreeSet<Node>>();
			HashMap<Node,TreeSet<Node>> newedges = new HashMap<Node,TreeSet<Node>>();

			Node[] line = null;

			boolean done = false;
			while(!done){
				if(_lastRead!=null){
					line = _lastRead;
					_lastRead=null;
				}
				else if(_in.hasNext()){
					line = _in.next();
				} else done = true;

				if(oldsubj == null)
					oldsubj = line[0];

				if(!oldsubj.equals(line[0]) || done){
					TreeSet<Node[]> newsa = calculateNewSameas(newedges, edges, _cards);
					if(newsa!=null && !newsa.isEmpty()){
						_current = newsa.iterator();
						_lastRead = line;
						return;
					}
					edges = new HashMap<Node,TreeSet<Node>>();
					newedges = new HashMap<Node,TreeSet<Node>>();
				}

				JoinIndexContext jic = JoinIndexContext.parse((Resource)line[3]);

				if(_subjLits || !(line[2] instanceof Literal)){
					if(jic.getRound()==_round-1){
						TreeSet<Node> newnodes = newedges.get(line[1]);
						if(newnodes==null){
							newnodes = new TreeSet<Node>();
							newedges.put(line[1], newnodes);
						}
						newnodes.add(line[2]);
					}else{
						TreeSet<Node> nodes = edges.get(line[1]);
						if(nodes==null){
							nodes = new TreeSet<Node>();
							edges.put(line[1], nodes);
						}
						nodes.add(line[2]);
					}
				}
				oldsubj = line[0];
			}
		}

		public TreeSet<Node[]> calculateNewSameas(Map<Node,? extends Set<Node>> newedges, Map<Node,? extends Set<Node>> oldedges, Map<Node,? extends Set<Node>> cards){
			TreeSet<Node[]> sameas = new TreeSet<Node[]>(NodeComparator.NC);

			if(newedges!=null && !newedges.isEmpty()){
				Set<Node> oldtypes = oldedges.get(RDF.TYPE);
				SameAsIndex sai = new SameAsIndex();
				if(oldtypes!=null) {
					for(Node n:oldtypes){
						Set<Node> preds = cards.get(n);

						if(preds!=null) for(Node p:preds){
							Set<Node> oldsame = oldedges.get(p);
							if(oldsame!=null && oldsame.size()>1){
								Node first = null;
								for(Node s:oldsame){
									if(first==null)
										first = s;
									else
										sai.addSameAs(first, s);
									//								sameas.add(new Node[]{first, OWL.SAMEAS, s, _context});
								}
							}
						}
					}

					for(Node n:oldtypes){
						Set<Node> preds = cards.get(n);
						if(preds!=null) for(Node p:preds){
							Set<Node> newsame = newedges.get(p);
							if(newsame!=null){
								Set<Node> oldsame = oldedges.get(p);
								Node first = null;
								for(Node s:newsame){
									if(first==null)
										first = s;
									else{
										if(sai.addSameAs(first, s))
											sameas.add(new Node[]{first, OWL.SAMEAS, s, _context});
									}
									//								sameas.add(new Node[]{first, OWL.SAMEAS, s, _context});
								}

								if(oldsame!=null){
									for(Node s:oldsame){
										if(sai.addSameAs(first, s))
											sameas.add(new Node[]{first, OWL.SAMEAS, s, _context});
									}
								}
							}
						}
					}
				}

				Set<Node> newtypes = newedges.get(RDF.TYPE);

				if(newtypes!=null) for(Node n:newtypes){
					Set<Node> preds = cards.get(n);
					if(preds!=null) for(Node p:preds){
						Set<Node> same = mergeSets(newedges.get(p), oldedges.get(p));
						if(same!=null){
							Node first = null;
							for(Node s:same){
								if(first==null)
									first = s;
								else{
									if(sai.addSameAs(first, s))
										sameas.add(new Node[]{first, OWL.SAMEAS, s, _context});
								}
							}
						}
					}
				}
			}
			return sameas;

		}

		public TreeSet<Node> mergeSets(Set<Node> a, Set<Node> b){
			TreeSet<Node> merged = new TreeSet<Node>();
			if(a!=null)
				merged.addAll(a);
			if(b!=null)
				merged.addAll(b);
			return merged;

		}
	}
}
