package topicDetector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import util.Configure;
import util.Document;
import util.DocumentRandomAccess;
import util.Parser;
import util.Document.Word;

/*
 * OPTICS is density based cluster algorithm.  Here we exploit to detect 
 * topics among tweets by clustering similar tweet together.
 * 
 */
public class OPTICSCluster implements Detector {
    static final Logger logger = Logger.getLogger(OPTICSCluster.class);



    // max epsilon for defining core object of OPTICS
    final float MaxEpsilon;

    // min points for defining core object
    final int MinPts;

    // epsilon for cluster determination
    final float Epsilon;

    final float UNDEF = 1f;

    Parser parser;

    public OPTICSCluster(Configure conf) {

	MaxEpsilon = conf.getMeps();
	Epsilon = conf.getEps();
	MinPts = conf.getMinPts();
    }

    public OPTICSCluster(Configure conf, Parser parser) throws IOException {
	MaxEpsilon = conf.getMeps();
	Epsilon = conf.getEps();
	MinPts = conf.getMinPts();

	this.parser = parser;
	this.parser.loadDictionary();

    }

    public Vector<Document> loadDocuments(String path, Vector<Document> docs)
	    throws FileNotFoundException {
	logger.info("loading file "+path+".txt");
	
	int k = 0;
	String line;
	long offset = 0, nextOffset = 0;

	//record offset of each tweet within the doc file
	DocumentRandomAccess docAccess = new DocumentRandomAccess();
	BufferedReader in = new BufferedReader(new FileReader(path+".txt"));

	try {
	    while ((line = in.readLine()) != null) {
		nextOffset += line.length() + 1;
		Document doc = parser.parseDocument(line);
		if (doc != null && doc.getWordVector().size() > 2) {
		    k++;
		    // record the offset of current tweet in the file
		    docAccess.addOffset(doc.getId(), offset);
		    docs.add(doc);
		}
		offset = nextOffset;
	    }
	    in.close();
	    docAccess.saveOffset(path);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return docs;
    }

    public Vector<Topic> loadTopics(String path, Vector<Topic> topics)
	    throws FileNotFoundException {

	String line;

	BufferedReader in = new BufferedReader(new FileReader(path+".topic"));
	long k = 1;
	try {
	    while ((line = in.readLine()) != null) {

		Topic topic = Topic.parse(line);
		topic.id = k;
		if (topic != null)
		    topics.add(topic);
	    }
	    in.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return topics;
    }

    /*
     * create index for a set of documents to accelerate topic detection. e.g.,
     * inverted index can help to cluster documents sharing one gram/word. other
     * clustering algorithm can then be conducted on these rough clusters.
     * 
     * @param docs a set of documents
     */
    public Vector<InvertedList> createDocIndex(Vector<Document> docs)
	    throws IOException {

	HashMap<Integer, InvertedList> invertedIndex = new HashMap<Integer, InvertedList>(
		parser.dict.dict.size() / 10);

	for (Document doc : docs) {
	    for (Word word : doc.getWordVector()) {
		InvertedList list = invertedIndex.get(word.getId());
		if (list == null) {
		    list = new InvertedList(word.getId());
		    invertedIndex.put(word.getId(), list);
		}
		list.addDoc(doc);
	    }
	}

	Vector<InvertedList> invertedLists = new Vector<InvertedList>(
		invertedIndex.values());

	return invertedLists;
    }

    public Vector<InvertedList> createTopicIndex(Vector<Topic> docs) {

	HashMap<Integer, InvertedList> invertedIndex = new HashMap<Integer, InvertedList>();

	for (Document doc : docs) {
	    for (Word word : doc.getWordVector()) {
		InvertedList list = invertedIndex.get(word.getId());
		if (list == null) {
		    list = new InvertedList(word.getId());
		}
		list.addDoc(doc);
	    }
	}

	Vector<InvertedList> invertedLists = new Vector<InvertedList>(
		invertedIndex.values());

	return invertedLists;
    }
    
    
    /*
     * merge topics of selected time windows
     * 
     * @param paths each path contains the topics for that time window
     * 
     * @return merged topics
     */
    public Vector<Topic> mergeTopics(String[] paths) throws IOException {
	logger.info("creat inverted index...");

	Vector<Topic> docs = new Vector<Topic>(100 * paths.length);
	for (String path : paths)
	    docs = loadTopics(path, docs);

	// inverted list for dictionary word
	Vector<InvertedList> invertedLists = createTopicIndex(docs);
	logger.info("finished creat inverted index.");
	logger.info(docs.size() + " documents have been indexed.");

	
	logger.info("cluser topics...");
	// generate ordered points according to max epsilon and min points for OPTICS
	
	String tmpPath = "tmp/" + System.currentTimeMillis() + ".orderedPoint";
	BufferedWriter out = new BufferedWriter(new FileWriter(tmpPath));
	for (InvertedList list : invertedLists)
	    doOPTICSCluster(list.docs, out);
	out.close();

	logger.info("topics have been clustered.");

	// determine clusters according to epsilon from ordered points
	BufferedReader in = new BufferedReader(new FileReader(tmpPath));

	Vector<Topic> topics = determineTopics(Epsilon, in);
	in.close();
	logger.info(topics.size() + " topics have been detected.");

	return topics;
    }

    /*
     * documents from the same inverted list share at least one word. thus we
     * cluster documents from the inverted list one by one
     */
    public Vector<Topic> detectTopics(String path) throws IOException {

	logger.info("creat inverted index...");

	Vector<Document> docs = new Vector<Document>(100000);
	docs = loadDocuments(path, docs);

	// inverted list for dictionary word
	Vector<InvertedList> invertedLists = createDocIndex(docs);

	logger.info(invertedLists.size() + " inverted index have been created.");
	logger.info(docs.size() + " documents have been indexed.");

	logger.info("cluser documents...");
	// generate ordered points according to max epsilon and min points for
	// OPTICS
	BufferedWriter out = new BufferedWriter(new FileWriter(path
		+ ".orderedPoint"));
	for (InvertedList pl : invertedLists)
	    doOPTICSCluster(pl.docs, out);
	out.close();

	logger.info("documents have been clustered.");

	// determine clusters according to epsilon from ordered points
	BufferedReader in = new BufferedReader(new FileReader(path
		+ ".orderedPoint"));
	Vector<Topic> topics = determineTopics(Epsilon, in);
	in.close();

	logger.info(topics.size() + " topics have been detected.");

	return topics;
    }

    /*
     * detect topics from a set of documents by OPTICS clustering (non-Javadoc)
     * 
     * @see topicDetector.Detector#detectTopics(java.util.Vector)
     */
    public Vector<Topic> detectTopics(Vector<Document> docs) throws IOException {
	BufferedWriter out = new BufferedWriter(new FileWriter(
		"tmp/tmp.orderedPoints"));
	doOPTICSCluster(docs, out);
	out.close();

	BufferedReader in = new BufferedReader(new FileReader(
		"tmp/tmp.orderedPoints"));

	Vector<Topic> topics = determineTopics(Epsilon, in);
	in.close();

	return topics;

    }

    /*
     * apply OPTICS clustering algorithm, save ordered points to tmp file
     * 
     * @param docs documents to be clustered
     */
    void doOPTICSCluster(Vector<Document> docs,
			 BufferedWriter orderedPointWriter) throws IOException {

	Vector<Point> points = new Vector<Point>(docs.size());
	int k = 0;
	for (Document doc : docs) {
	    Point p = new Point(doc, k++);
	    points.add(p);
	}

	Vector<Point> neighbor = new Vector<Point>();
	Vector<Float> neighborDist = new Vector<Float>();
	for (Point p : points) {
	    if (!hasProcessed(p)) {

		int size = getEpsNeighbor(p, points, neighbor, neighborDist);
		p.save(orderedPointWriter);
		setProcessed(p);
		if (size >= MinPts) {
		    MinHeap orderedSeed = new MinHeap(neighbor.size());
		    updateOrderedSeed(p, neighbor, neighborDist, orderedSeed);

		    while (!orderedSeed.isEmpty()) {
			Point q = orderedSeed.getMinPoint();
			size = getEpsNeighbor(q, points, neighbor, neighborDist);
			q.save(orderedPointWriter);
			setProcessed(q);

			if (size >= MinPts) {
			    updateOrderedSeed(q, neighbor, neighborDist,
				    orderedSeed);
			}
		    }
		}
	    }
	}

	orderedPointWriter.flush();
    }

    /*
     * get the epsilon neighborhood and set the core distance if it exist;
     * 
     * @param p current point
     * 
     * @param vec the set of all points
     * 
     * @param neighbor all epsilon neighbors, to be inserted
     * 
     * @param neighborDist distance of epsilon neighbor
     * 
     * @return the size of the epsilon neighbor
     */
    public int getEpsNeighbor(Point p, Vector<Point> set,
			      Vector<Point> neighbor, Vector<Float> neighborDist) {
	int size = p.getSize();
	float dist;

	// to sort distances, and get the core distance
	Vector<DistSizePair> distSize = new Vector<DistSizePair>();

	neighbor.clear();
	neighborDist.clear();

	for (Point q : set) {
	    if (hasProcessed(q) || p == q)
		continue;
	    dist = p.distanceTo(q);
	    if (dist < MaxEpsilon) {
		neighbor.add(q);
		neighborDist.add(dist);
		size += q.getSize();

		distSize.add(new DistSizePair(q.getSize(), dist));
	    }
	}

	if (size < MinPts)
	    return size;

	Collections.sort(distSize);

	int k = p.getSize();
	int i = 0;
	while (k < MinPts)
	    k += distSize.get(i++).size;

	if (i > 0)
	    p.coreDist = distSize.get(i - 1).dist;
	else
	    p.coreDist = 0f;

	return size;
    }

    /*
     * update the ordered seed set by updating reachability distance for each
     * neighbor sort neighbors according to reachability distance in ascending
     * order
     * 
     * @param curr the current core object point
     * 
     * @param neighbor neighbor points of the curr point
     * 
     * @param neighborDist distance to each neighbor point
     * 
     * @param orderedSeed to be updated
     */
    void updateOrderedSeed(Point curr, Vector<Point> neighbor,
			   Vector<Float> neighborDist, MinHeap orderedSeed) {
	float newReachDist;
	float coreDist = curr.coreDist;
	for (int i = 0; i < neighbor.size(); i++) {
	    Point p = neighbor.get(i);
	    float dist = neighborDist.get(i);
	    newReachDist = Math.max(coreDist, dist);
	    if (p.reachDist == UNDEF) {
		p.reachDist = newReachDist;
		orderedSeed.addPoint(p);
	    } else if (newReachDist < p.reachDist) {
		p.reachDist = newReachDist;
	    }
	}

    }

    boolean hasProcessed(Point p) {
	return p.doc.getSupport() == 0;
    }

    void setProcessed(Point p) {
	p.doc.clearSupport();
    }

    /*
     * read ordered points from tmp file, assign it to a cluster based on
     * epsilon threshold
     * 
     * @param epsilon threshold for OPTICS clustering
     * 
     * @return topics sorted by its popularity in descending order
     */
    Vector<Topic> determineTopics(float epsilon,
				  BufferedReader orderedPointReader)
	    throws IOException {

	Vector<Topic> topics = new Vector<Topic>();
	Topic tp = null;

	String line;
	while ((line = orderedPointReader.readLine()) != null) {
	    Point p = new Point(line);
	    if (p.reachDist > epsilon) {
		if (p.coreDist <= epsilon) {
		    tp = new Topic();
		    tp.addDoc(p.doc);
		    topics.add(tp);
		}
	    } else {
		tp.addDoc(p.doc);
	    }
	}
	Collections.sort(topics, new TopicComp());

	return topics;
    }

    /*
     * save topics into disk
     * 
     * @param path tweet file path(not path for storing topics, which is path+".topics")
     */
    public void saveTopics(Vector<Topic> topics, String path)
	    throws IOException {
	
	BufferedWriter out = new BufferedWriter(new FileWriter(path+".topic"));
	for (Topic tp : topics) {
	    // generate popular words of the topic
	    tp.aggregateDocs();
	    out.write(tp.toString());
	    out.newLine();
/*
	    System.out.println("popularity:" + tp.getSupport());
	    for (Document doc : tp.getPopularDocs())
		System.out.println(doc.getSupport() + "\t"
			+ docAccess.readDocument(doc.getId()));

	    System.out.println();
*/
	}
	out.flush();
	out.close();

    }
    
    
    /*
     * encode detected topics ordered by popularity
     * 
     * @param topics detected topics
     * @path original tweet file path
     * 
     * @return a string representation for transmit
     */
    @SuppressWarnings("unchecked")
    public String encodeTopics(Vector<Topic> topics, String[] paths) throws NumberFormatException, IOException{
	HashMap<Integer,DocumentRandomAccess> docAccess=new HashMap<Integer,DocumentRandomAccess>(paths.length*2);
	
	for(String path:paths){
	    DocumentRandomAccess docAcs = new DocumentRandomAccess(path);
	    docAcs.startRead(path);
	    
	    File f=new File(path);
	    int slot=Integer.parseInt(f.getName());
	    docAccess.put(slot, docAcs);
	}
	
	JSONArray jsonary=new JSONArray();
	
	
	for (Topic tp : topics) {
	    // generate popular words of the topic
	    tp.aggregateDocs();
	    
	    JSONObject obj=new JSONObject();
	    obj.put("pop", tp.getSupport());
	    
	    JSONArray popDocs=new JSONArray();
	    for (Document doc : tp.getPopularDocs())
		popDocs.add(docAccess.get(doc.getSlot()).readDocument(doc.getId()));	    
	    obj.put("docs", popDocs);
	    
	    jsonary.add(obj);
	}

	for(DocumentRandomAccess docAcs:docAccess.values())
	    docAcs.endRead();
	
	return jsonary.toJSONString();
    }

    class MinHeap {
	Vector<Point> points;
	int vacantPos;

	public MinHeap(int size) {
	    points = new Vector<Point>(size);
	    vacantPos = -1;
	}

	public void addPoint(Point point) {
	    if (vacantPos >= 0) {
		points.set(vacantPos, point);
		vacantPos = -1;
	    } else
		points.add(point);
	}

	public Point getMinPoint() {
	    if (vacantPos >= 0) {
		points.set(vacantPos, points.lastElement());
		points.remove(points.lastElement());
		vacantPos = -1;
	    }

	    float minReachDist = 1f;

	    for (int i = 0; i < points.size(); i++) {
		if (minReachDist > points.get(i).reachDist) {
		    minReachDist = points.get(i).reachDist;
		    vacantPos = i;
		}
	    }

	    return points.get(vacantPos);
	}

	public boolean isEmpty() {
	    return points.isEmpty() || (points.size() == 1 && vacantPos >= 0);
	}
    }

    class TopicComp implements Comparator<Topic> {
	public int compare(Topic t1, Topic t2) {
	    return t2.getSupport() - t1.getSupport();
	}
    }

    class DistanceCalculator {
	HashMap<Long, Float> distCache;

	public DistanceCalculator(int size) {
	    init(size);
	}

	public void init(int size) {
	    distCache = new HashMap<Long, Float>(size);
	}

	public void clear() {
	    distCache.clear();
	}

	public float distance(Point a, Point b) {
	    long ida = a.id, idb = b.id;
	    long compose = a.id < b.id ? (ida << 31) + idb : (idb << 31) + ida;
	    Float ret = distCache.get(compose);
	    if (ret == null) {
		ret = a.distanceTo(b);
		distCache.put(compose, ret);
	    } else {
		System.out.println();
	    }

	    return ret;

	}
    }

    class DistSizePair implements Comparable<DistSizePair> {
	int size;
	float dist;

	public DistSizePair(int s, float d) {
	    size = s;
	    dist = d;
	}

	public int compareTo(DistSizePair other) {
	    if (this.dist < other.dist)
		return -1;
	    else
		return 1;
	}
    }

    class Point implements Comparable<Point> {
	int id;
	float coreDist, reachDist;
	Document doc;

	public Point(Document doc, int id) {
	    this.id = id;
	    this.doc = doc;
	    reachDist = UNDEF;
	    coreDist = UNDEF;
	}

	public Point(String str) {
	    int idx1 = str.indexOf(",");
	    coreDist = Float.parseFloat(str.substring(0, idx1));

	    int idx2 = str.indexOf(",", idx1 + 1);
	    reachDist = Float.parseFloat(str.substring(idx1 + 1, idx2));

	    doc = new Document();
	    doc.decode(str.substring(idx2 + 1));
	}

	float distanceTo(Point other) {
	    return 1f - doc.sim(other.doc);
	}

	int getSize() {
	    return doc.getSupport();
	}

	public int compareTo(Point other) {
	    if (this.reachDist > other.reachDist)
		return -1;
	    else
		return 1;
	}

	void save(BufferedWriter out) throws IOException {
	    String ret = coreDist + "," + reachDist + "," + doc.toString();
	    out.write(ret);
	    out.newLine();
	}
    }

    class InvertedList implements Comparable<InvertedList> {
	int id;// id of the gram, from the dictionary
	       // float idf;
	int size;

	Vector<Document> docs;

	public InvertedList(int id) {
	    this.id = id;

	    this.size = 0;

	    this.docs = new Vector<Document>();
	}

	public void addDoc(Document doc) {
	    docs.add(doc);
	    size += doc.getSupport();
	}

	/*
	 * public float getTfIdf() { return size;// * idf; }
	 */

	public int compareTo(InvertedList other) {
	    if (this.size < other.size) {
		return 1;
	    } else
		return -1;
	}

    }

    public static void main(String[] args) throws IOException {
	Configure conf = new Configure("config/config.xml");

	OPTICSCluster optics = new OPTICSCluster(conf);

	BufferedReader in = new BufferedReader(new FileReader(
		"data/test/10.txt.orderedPoints"));
	Vector<Topic> topics = optics.determineTopics(0.2f, in);
	optics.saveTopics(topics, "data/test/10");
	in.close();

    }
}
