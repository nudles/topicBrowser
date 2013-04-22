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

    
    /*
     * add document to this topic
     * the document's support is aggregated
     * the popular document queue is updated
     * 
     * @doc the document to be add
     */
    public void addDoc(Document doc) {
	docs.add(doc);
	support += doc.getSupport();
	
	//if doc is a topic, then all its popular docs are added to current topic
	if (doc.getClass().equals(Topic.class)) {
	    Topic tp = (Topic) doc;
	    for (Document d : tp.popularDocs) {
		updatePopularDoc(d);
	    }
	} else
	    updatePopularDoc(doc);
    }

    /*
     * update the popular document queue
     * 
     * @doc the document that may be inserted into the queue
     */
    public void updatePopularDoc(Document doc) {
	
	if (popularDocs.size() < popularDocN)
	    popularDocs.add(doc);	
	else if (popularDocs.peek().getSupport() < doc.getSupport()
		|| (popularDocs.peek().getSupport() == doc.getSupport() && Math
			.random() > 0.5)) {
	    //if queue is full and the top's support == doc's support, either keep it or replace it
	    
	    popularDocs.poll();
	    popularDocs.add(doc);
	}
    }
    
    

    public PriorityQueue<Document> getPopularDocs() {
	return popularDocs;
    }

    
    /*
     * encode the topic into a string
     * 
     * @return a string represent the topic
     */
    public String toString() {

	String ret = String.valueOf(popularDocs.size());
	for (Document d : popularDocs)
	    ret += "," + d.getId();

	ret += "," + super.toString();

	return ret;
    }
    

    /*
     * parse topic string, which is of the format: popularDocNum {,popularDocId} encoded document string
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

    /*
     * summarize the word distribution for the topic
     * normalize it
     */
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

	// set topic's time as any doc's time, just for its time window determination
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


    class WordWeightComp implements Comparator<Word> {
	public int compare(Word w1, Word w2) {
	    if (w1.getWeight() > w2.getWeight())
		return -1;
	    else
		return 1;
	}
    }

}
