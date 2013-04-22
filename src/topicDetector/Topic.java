package topicDetector;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Vector;
import util.Document;

public class Topic extends Document {

    private Vector<Document> docs;
    private PriorityQueue<Document> popularDocs;

    private final int popularDocN = 20;
    private final int popularWordN = 20;

    public Topic() {
	docs = new Vector<Document>();
	popularDocs = new PriorityQueue<Document>(popularDocN);
    }

    public Topic(int popDocN) {
	popularDocs = new PriorityQueue<Document>(popDocN);
    }

    public void addDoc(Document doc) {
	docs.add(doc);
	support += doc.getSupport();
	if (doc.getClass().equals(Topic.class)) {
	    Topic tp = (Topic) doc;
	    for (Document d : tp.popularDocs) {
		addPopularDoc(d);
	    }
	} else
	    addPopularDoc(doc);
    }

    public void addPopularDoc(Document doc) {
	if (popularDocs.size() < popularDocN)
	    popularDocs.add(doc);
	else if (popularDocs.peek().getSupport() < doc.getSupport()
		|| (popularDocs.peek().getSupport() == doc.getSupport() && Math
			.random() > 0.5)) {
	    popularDocs.poll();
	    popularDocs.add(doc);
	}
    }

    public PriorityQueue<Document> getPopularDocs() {
	return popularDocs;
    }

    /*
     * sum documents' support of this topic. average the word vector. sample
     * some documents as representatives.
     * 
     * @param writer write the aggregated topic into file stream
     */
    public String toString() {

	String ret = String.valueOf(popularDocs.size());
	for (Document d : popularDocs)
	    ret += "," + d.getId();

	ret += "," + super.toString();

	return ret;
    }

    /*
     * parse topic string, which is of the format: popularDocNum {,popularDocId}
     * encoded document string
     * 
     * @param tpStr topic encoded in string
     */
    public static Topic parse(String tpStr) {
	int pos = tpStr.indexOf(",");
	int popDocN = Integer.parseInt(tpStr.substring(0, pos));
	Topic tp = new Topic(popDocN);

	int next;
	for (int i = 0; i < popDocN; i++) {
	    Document doc = new Document();
	    next = tpStr.indexOf(",",pos+1);
	    doc.id = Long.parseLong(tpStr.substring(pos + 1, next));
	    tp.popularDocs.add(doc);
	    pos = next;
	}

	tp.decode(tpStr.substring(pos + 1));
	return tp;
    }

    public void aggregateDocs() {
	vec = aggregate(docs);
	normalize();
    }

    /*
     * aggregate word frequency from docs of this topic; set topic's time as any
     * doc's time, just for its time window determination
     */
    Vector<Word> aggregate(List<Document> docs) {
	HashMap<Integer, Word> wordFreq = new HashMap<Integer, Word>(
		docs.size() * 10);

	// set topic's time as any doc's time, just for its time window
	// determination
	time = docs.get(0).getTime();
	for (Document doc : docs) {
	    for (Word word : doc.getWordVector()) {
		Word w = wordFreq.get(word.getId());
		if (w == null)
		    w = new Word(word.getId(), word.getWeight());
		else
		    w.setWeight(w.getWeight() + word.getWeight());
		wordFreq.put(word.getId(), w);
	    }
	}

	PriorityQueue<Word> popularWords = new PriorityQueue<Word>(
		popularWordN, new WordWeightComp());

	for (Word word : wordFreq.values()) {
	    if (popularWords.size() < popularWordN)
		popularWords.add(word);
	    else if (popularWords.peek().getWeight() < word.getWeight()) {
		popularWords.poll();
		popularWords.add(word);
	    }
	}

	return new Vector<Word>(popularWords);

    }

    Document merge(Document doc1, Document doc2) {
	Vector<Word> vec1 = doc1.getWordVector();
	Vector<Word> vec2 = doc2.getWordVector();

	Vector<Word> retVec = new Vector<Word>(vec1.size() + vec2.size());

	int id1 = vec1.get(0).getId();
	int id2 = vec2.get(0).getId();
	int i = 1, j = 1;

	while (true) {
	    if (id1 == id2) {
		Word w = new Word(id1, vec1.get(i - 1).getWeight()
			+ vec2.get(j - 1).getWeight());
		retVec.add(w);

		if (i < vec1.size() && j < vec2.size()) {
		    id1 = vec1.get(i++).getId();
		    id2 = vec2.get(j++).getId();
		} else
		    break;
	    } else if (id1 < id2) {
		retVec.add(new Word(vec1.get(i - 1)));
		if (i < vec1.size())
		    id1 = vec1.get(i++).getId();
		else {
		    j--;
		    break;
		}

	    } else {
		retVec.add(new Word(vec2.get(j - 1)));
		if (j < vec2.size())
		    id2 = vec2.get(j++).getId();
		else {
		    i--;
		    break;
		}
	    }
	}

	while (i < vec1.size())
	    retVec.add(new Word(vec1.get(i)));

	while (j < vec2.size())
	    retVec.add(new Word(vec2.get(j)));

	Document doc = new Document();

	doc.setSupport(doc1.getSupport() + doc2.getSupport());
	doc.setWordVector(retVec);

	return doc;
    }

    class WordWeightComp implements Comparator<Word> {
	public int compare(Word w1, Word w2) {
	    if (w1.getWeight() > w2.getWeight())
		return -1;
	    else
		return 1;
	}
    }

}
