package org.semanticweb.swse.qp.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.semanticweb.nxindex.NodesIndex;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.nxindex.sparse.SparseIndex;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.GEO;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.semanticweb.yars.nx.parser.NxParser;

public class QueryProcessor {
	private Searcher _kws;
	private NodesBlockReaderNIO _nbrio;
	private NodesIndex _ni;

	private Directory _dir;

	public static final String DEFAULT_LANG = "en";
	public static final String NULL_LANG = "en"; 
	
	public static final Resource AUTH_CONTEXT = new Resource("http://swse.deri.org/#auth");
	public static final Resource NON_AUTH_CONTEXT = new Resource("http://swse.deri.org/#non-auth");
	
	public static final Resource CONTEXT = new Resource("http://swse.deri.org/");

	public QueryProcessor(String spoc, String sparse, String lucene) throws IOException{
		_nbrio = new NodesBlockReaderNIO(spoc);
		SparseIndex sp = new SparseIndex(sparse);
		_ni = new NodesIndex(_nbrio, sp);
		_dir = new NIOFSDirectory(new File(lucene));
		_kws = new IndexSearcher(_dir, true);
	}

	public void close() throws IOException{
		_kws.close();
		_nbrio.close();
		_dir.close();
	}

	public Iterator<Node[]> getSnippets(String keywordQ, int from, int to) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return getSnippets(keywordQ, from, to, DEFAULT_LANG);
	}
	
	public Iterator<Node[]> getSnippets(String keywordQ, int from, int to, String prefLangPrefix) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		ScoreDoc[] sd = getDocs(keywordQ, from, to);
		return getSnippets(sd, prefLangPrefix);
	}

	public Iterator<Node[]> getSnippets(ScoreDoc[] hits) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return getSnippets(hits, DEFAULT_LANG);
	}
	
	public Iterator<Node[]> getSnippets(ScoreDoc[] hits, String prefLangPrefix) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return new KeywordResults(hits, _kws, prefLangPrefix);
	}

	public Iterator<Node[]> getFocus(Node sub) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return new FocusResult(sub, _ni);
	}

	public Iterator<Node[]> getEntity(Node n) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return getEntity(n, DEFAULT_LANG);
	}
	
	public Iterator<Node[]> getEntity(Node n, String prefLangPrefix) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return new EntityResult(n, _kws, prefLangPrefix);
	}

	public Iterator<Node[]> getEntities(Collection<Node> ns) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return getEntities(ns, DEFAULT_LANG);
	}
	
	public Iterator<Node[]> getEntities(Collection<Node> ns, String prefLangPrefix) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ParseException{
		return new EntityResults(ns, _kws, prefLangPrefix);
	}

	/**
	 *
	 * @param keywordQ
	 * @param from 1 inclusive
	 * @param to 10 inclusive
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	public ScoreDoc[] getDocs(String keywordQ, int from, int to) throws ParseException, IOException{
		Analyzer sa = new StandardAnalyzer(Version.LUCENE_CURRENT);

		QueryParser qp = new MultiFieldQueryParser(
				Version.LUCENE_CURRENT, 
				new String[] { 
						LuceneIndexBuilder.DocumentRepresentation.KEYWORDS,
						LuceneIndexBuilder.DocumentRepresentation.LABEL_TEXT
				}, 
				sa);

		qp.setDefaultOperator(QueryParser.Operator.AND);

		Query query = qp.parse(keywordQ);

		ScoreDoc[] hits = _kws.search(query, to).scoreDocs;

		if(from<=0){
			return hits;
		} else if(hits.length<from){
			return new ScoreDoc[0];
		}else{
			from = from - 1;
			int len = hits.length-from;

			ScoreDoc[] slice = new ScoreDoc[len];
			System.arraycopy(hits, from, slice, 0, len);

			return slice;
		}
	}

	public static class FocusResult implements Iterator<Node[]>{
		public final Resource RANK_PRED = new Resource("http://swse.deri.org/#luceneRank");
		public final Resource CONTEXT = new Resource("http://swse.deri.org/");
		public final Resource FOCUS_TYPE = new Resource("http://swse.deri.org/#Focus");

		public final Resource ID_RANK_PRED = new Resource("http://swse.deri.org/#idRank");

		private Iterator<Node[]> _subIter = null;
		private Iterator<Node[]> _first = null;

		public FocusResult(Node n, NodesIndex ni) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException, ParseException{
			Node[] key = new Node[]{n};
			_subIter = ni.getIterator(key);

			ArrayList<Node[]> first = new ArrayList<Node[]>();
			first.add(new Node[]{n, RDF.TYPE, FOCUS_TYPE, CONTEXT});

			_first = first.iterator();
		}

		public boolean hasNext() {
			if(_first!=null && _first.hasNext()){
				return true;
			} else if(_subIter!=null && _subIter.hasNext()){
				return true;
			}
			return false;
		}

		public Node[] next() {
			if(_first!=null && _first.hasNext()){
				return _first.next();
			}
			if(_subIter!=null && _subIter.hasNext()){
				return _subIter.next();
			}
			throw new NoSuchElementException();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}


	public static class EntityResult implements Iterator<Node[]>{
		public final Resource ID_RANK_PRED = new Resource("http://swse.deri.org/#idRank");

		public final Resource CONTEXT = new Resource("http://swse.deri.org/");

		private Iterator<Node[]> _first = null;

		public EntityResult(Node n, Searcher s, String prefLang) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException, ParseException{
			Term t = new Term(LuceneIndexBuilder.DocumentRepresentation.SUBJECT, n.toN3());
			TermQuery query = new TermQuery(t);

			ScoreDoc[] hits = s.search(query, 2).scoreDocs;
			ScoreDoc hit = null;
			if(hits!=null && hits.length!=0){
				if(hits.length>1){
					System.err.println("More than one hit for subj "+n);
				}
				hit = hits[0];
			} else{
				return;
			}

			Document d = s.doc(hit.doc);

			ArrayList<Node[]> first = new ArrayList<Node[]>();
//			first.add(new Node[]{n, LUCENE_RANK_PRED, new Literal(Float.toString(hit.score)), CONTEXT});

			ArrayList<Literal> authlabs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_LABELS));
			ArrayList<Literal> labs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LABELS));
			
			Literal lab = chooseLabelOrComment(authlabs, labs, prefLang);
			if(lab!=null){
				first.addAll(toStmts(n, RDFS.LABEL, CONTEXT, lab));
			}
			
//			ArrayList<Literal> authcoms = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_COMMENTS));
//			ArrayList<Literal> coms = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.COMMENTS));
//			
//			Literal com = chooseLabelOrComment(authcoms, coms, prefLang);
//			if(com!=null){
//				first.addAll(toStmts(n, RDFS.COMMENT, CONTEXT, com));
//			}

			Field f = d.getField(LuceneIndexBuilder.DocumentRepresentation.RANK);
			first.add(new Node[]{n, ID_RANK_PRED, new Literal(f.stringValue()), CONTEXT});

//			ArrayList<Resource> imgs = createResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.IMG));
//			first.addAll(toStmts(n, FOAF.DEPICTION, CONTEXT, imgs));

//			ArrayList<Resource> authtypes = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_TYPES));
//			first.addAll(toStmts(n, RDF.TYPE, AUTH_CONTEXT, authtypes));
//			
//			ArrayList<Resource> types = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.TYPES));
//			first.addAll(toStmts(n, RDF.TYPE, NON_AUTH_CONTEXT, types));
			
			ArrayList<Resource> sameas = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.SAMEAS));
			first.addAll(toStmts(n, OWL.SAMEAS, CONTEXT, sameas));
			
			ArrayList<Literal> lats = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LAT));
			first.addAll(toStmts(n, GEO.LAT, CONTEXT, lats));

			ArrayList<Literal> longs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LONG));
			first.addAll(toStmts(n, GEO.LONG, CONTEXT, longs));
			
			_first = first.iterator();
		}

		public boolean hasNext() {
			if(_first!=null && _first.hasNext()){
				return true;
			}
			return false;
		}

		public Node[] next() {
			if(_first!=null && _first.hasNext()){
				return _first.next();
			}
			throw new NoSuchElementException();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static class KeywordResult implements Iterator<Node[]>{
		public static final Resource LUCENE_RANK_PRED = new Resource("http://swse.deri.org/#luceneRank");
		public static final Resource ID_RANK_PRED = new Resource("http://swse.deri.org/#idRank");

		public static final Resource KEYWORD_HIT = new Resource("http://swse.deri.org/#KeywordHit");

		private Iterator<Node[]> _first = null;

		public KeywordResult(ScoreDoc sd, Searcher s, String prefLang) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException{
			Document d = s.doc(sd.doc);

			String sub = d.get(LuceneIndexBuilder.DocumentRepresentation.SUBJECT);
			Node n = NxParser.parseNode(sub);
			
			ArrayList<Node[]> first = new ArrayList<Node[]>();
			first.add(new Node[]{n, LUCENE_RANK_PRED, new Literal(Float.toString(sd.score)), CONTEXT});
			first.add(new Node[]{n, RDF.TYPE, KEYWORD_HIT, CONTEXT});

			ArrayList<Literal> authlabs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_LABELS));
			ArrayList<Literal> labs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LABELS));
			
			Literal lab = chooseLabelOrComment(authlabs, labs, prefLang);
			if(lab!=null){
				first.addAll(toStmts(n, RDFS.LABEL, CONTEXT, lab));
			}
			
			ArrayList<Literal> authcoms = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_COMMENTS));
			ArrayList<Literal> coms = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.COMMENTS));
			
			Literal com = chooseLabelOrComment(authcoms, coms, prefLang);
			if(com!=null){
				first.addAll(toStmts(n, RDFS.COMMENT, CONTEXT, com));
			}

			Field f = d.getField(LuceneIndexBuilder.DocumentRepresentation.RANK);
			first.add(new Node[]{n, ID_RANK_PRED, new Literal(f.stringValue()), CONTEXT});

			ArrayList<Resource> imgs = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.IMG));
			first.addAll(toStmts(n, FOAF.DEPICTION, CONTEXT, imgs));

			ArrayList<Resource> authtypes = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.AUTH_TYPES));
			first.addAll(toStmts(n, RDF.TYPE, AUTH_CONTEXT, authtypes));
			
			ArrayList<Resource> types = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.TYPES));
			first.addAll(toStmts(n, RDF.TYPE, NON_AUTH_CONTEXT, types));
			
			ArrayList<Resource> sameas = parseResources(d.getFields(LuceneIndexBuilder.DocumentRepresentation.SAMEAS));
			first.addAll(toStmts(n, OWL.SAMEAS, CONTEXT, sameas));
			
			ArrayList<Literal> lats = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LAT));
			first.addAll(toStmts(n, GEO.LAT, CONTEXT, lats));

			ArrayList<Literal> longs = parseLiterals(d.getFields(LuceneIndexBuilder.DocumentRepresentation.LONG));
			first.addAll(toStmts(n, GEO.LONG, CONTEXT, longs));

			_first = first.iterator();
		}

		public boolean hasNext() {
			if(_first!=null && _first.hasNext()){
				return true;
			}
			return false;
		}

		public Node[] next() {
			if(_first!=null && _first.hasNext()){
				return _first.next();
			}
			throw new NoSuchElementException();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static class KeywordResults extends Results{
		public KeywordResults(ScoreDoc[] sds, Searcher s, String prefLang) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException{
			ArrayList<KeywordResult> results = new ArrayList<KeywordResult>();

			for(ScoreDoc sd:sds){
				results.add(new KeywordResult(sd, s, prefLang));
			}
			_all = results.iterator();

			loadNext();
		}
	}

	public static class EntityResults extends Results {
		public EntityResults(Collection<Node> ns, Searcher s, String prefLang) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException, ParseException{
			ArrayList<EntityResult> results = new ArrayList<EntityResult>();

			for(Node n:ns){
				results.add(new EntityResult(n, s, prefLang));
			}
			_all = results.iterator();

			loadNext();
		}
	}

	public static class Results implements Iterator<Node[]>{
		protected Iterator<? extends Iterator<Node[]>> _all = null;
		private Iterator<Node[]> _current = null;

		protected Results(){
			;
		}

		public Results(Collection<? extends Iterator<Node[]>> iters) throws org.semanticweb.yars.nx.parser.ParseException, CorruptIndexException, IOException{
			_all = iters.iterator();

			loadNext();
		}

		protected void loadNext(){
			if(_current!=null && _current.hasNext())
				return;

			if(_all!=null) while(_all.hasNext() && (_current==null || !_current.hasNext())){
				_current = _all.next();	
			}

			if(_current!=null && !_current.hasNext()){
				_current = null;
			}
		}

		public boolean hasNext() {
			loadNext();
			return _current != null;
		}

		public Node[] next() {
			loadNext();
			if(_current==null){
				throw new NoSuchElementException();
			}
			return _current.next();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

//	private static Literal chooseLabelOrComment(ArrayList<Literal> auths, ArrayList<Literal> nonauths){
//		return chooseLabelOrComment(auths, nonauths, DEFAULT_LANG);
//	}

	public static Literal chooseLabelOrComment(ArrayList<Literal> auths, ArrayList<Literal> nonauths, String langPrefix){
		Literal best = null;
		boolean auth = false;
		if(auths!=null) for(Literal l:auths){
			best = chooseLabelOrComment(best, true, l, true, langPrefix);
			auth = true;
		}
		if(nonauths!=null) for(Literal l:nonauths){
			Literal newbest = chooseLabelOrComment(best, auth, l, false, langPrefix);
			if(newbest!=best){
				auth = false;
				best = newbest;
			}
		}
		
		return best;
	}

//	private static Literal chooseLabelOrComment(Literal a, Literal b){
//		return chooseLabelOrComment(a, false, b, false, DEFAULT_LANG);
//	}
//	
//	private static Literal chooseLabelOrComment(Literal a, Literal b, String langPrefix){
//		return chooseLabelOrComment(a, false, b, false, langPrefix);
//	}
//	
//	private static Literal chooseLabelOrComment(Literal a, boolean authA, Literal b, boolean authB){
//		return chooseLabelOrComment(a, authA, b, authB, DEFAULT_LANG);
//	}
	
	/**
	 * Some rough criteria for choosing one literal/comment over another
	 * @param a
	 * @param authA
	 * @param b
	 * @param authB
	 * @param langPrefix
	 * @return
	 */
	private static Literal chooseLabelOrComment(Literal a, boolean authA, Literal b, boolean authB, String langPrefix){
		if(a==null)
			return b;
		else if(b==null)
			return a;
		
		if(a.getData().isEmpty())
			return b;
		else if(b.getData().isEmpty()){
			return a;
		}
		
		if(matches(a.getLanguageTag(),langPrefix)){
			if(!matches(b.getLanguageTag(), langPrefix)){
				return a;
			}
		} else if(matches(b.getLanguageTag(), langPrefix)){
			return b;
		}
		
		if(authA && !authB){
			return a;
		} else if(authB && !authA){
			return b;
		}
		
		if(a.getData().length()<b.getData().length()){
			return a;
		} else return b;
	}
	
	private static boolean matches(String lang, String langTag){
		if(lang == null)
			return true;

		if(langTag==null){
			langTag = NULL_LANG;
		}

		if(langTag.toLowerCase().startsWith(lang.toLowerCase())){
			return true;
		}
		return false;
	}
	
//	private static ArrayList<Resource> createResources(Field... fs){
//		ArrayList<Resource> nodes = new ArrayList<Resource>();
//		if(fs!=null) for(Field f:fs){
//			if(f!=null)
//				nodes.add(new Resource(f.stringValue()));
//		}
//		return nodes;
//	}
//	
//	private static ArrayList<Literal> createLiterals(Field... fs){
//		ArrayList<Literal> nodes = new ArrayList<Literal>();
//		if(fs!=null) for(Field f:fs){
//			if(f!=null)
//				nodes.add(new Literal(f.stringValue()));
//		}
//		return nodes;
//	}

//	private static ArrayList<Node> parseNodes(Field... fs){
//		ArrayList<Node> nodes = new ArrayList<Node>();
//		if(fs!=null) for(Field f:fs){
//			try{
//				nodes.add(NxParser.parseNode(f.stringValue()));
//			} catch (org.semanticweb.yars.nx.parser.ParseException e) {
//				System.err.println("Error parsing result field "+fs);
//				e.printStackTrace();
//			}
//		}
//		return nodes;
//	}
	
	private static ArrayList<Literal> parseLiterals(Field... fs){
		ArrayList<Literal> nodes = new ArrayList<Literal>();
		if(fs!=null) for(Field f:fs){
			try{
				Node n = NxParser.parseNode(f.stringValue());
				if(n instanceof Literal){
					nodes.add((Literal)n);
				}
			} catch (org.semanticweb.yars.nx.parser.ParseException e) {
				System.err.println("Error parsing result field "+fs);
				e.printStackTrace();
			}
		}
		return nodes;
	}
	
	private static ArrayList<Resource> parseResources(Field... fs){
		ArrayList<Resource> nodes = new ArrayList<Resource>();
		if(fs!=null) for(Field f:fs){
			try{
				Node n = NxParser.parseNode(f.stringValue());
				if(n instanceof Resource){
					nodes.add((Resource)n);
				}
			} catch (org.semanticweb.yars.nx.parser.ParseException e) {
				System.err.println("Error parsing result field "+fs);
				e.printStackTrace();
			}
		}
		return nodes;
	}

	private static ArrayList<Node[]> toStmts(Node subj, Node pred, Node con, Node... objs) throws org.semanticweb.yars.nx.parser.ParseException{
		ArrayList<Node[]> nodes = new ArrayList<Node[]>();
		if(nodes==null) return nodes;

		for(Node obj:objs)
			nodes.add(new Node[]{subj, pred, obj, con});

		return nodes;
	}
	
	private static ArrayList<Node[]> toStmts(Node subj, Node pred, Node con, Collection<? extends Node> objs) throws org.semanticweb.yars.nx.parser.ParseException{
		ArrayList<Node[]> nodes = new ArrayList<Node[]>();
		if(nodes==null) return nodes;

		for(Node obj:objs)
			nodes.add(new Node[]{subj, pred, obj, con});

		return nodes;
	}
}
