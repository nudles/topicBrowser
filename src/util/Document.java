package util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Document implements Comparable<Document> {
    public long id;
    protected long time;
    protected int support;
    protected Vector<Word> vec;

    public Document(long id, int support, long time) {
	this.id = id;
	this.support = support;
	this.time = time;
    }

    public Document() {

    }

    public void initWordVector(int size) {
	vec = new Vector<Word>(size);
    }

    /*
     * @return the word vector of this document
     */
    public Vector<Word> getWordVector() {
	return vec;
    }

    public void setWordVector(Vector<Word> v) {
	vec = v;
    }

    /*
     * return the slot of the document, which is calculated as
     * (created_time-start_time)/time_window
     * 
     * @return slot of the document
     */
    public int getSlot() {
	return Configure.getSlot(time);
    }

    public long getTime(){
	return time;
    }

    /*
     * return the id of the document, e.g., tweet id
     * 
     * @return id of the document
     */
    public long getId() {
	return id;
    }

    public int getSupport() {
	return support;
    }

    public void setSupport(int support) {
	this.support = support;
    }

    /*
     * increment the support of the document by one
     */
    public void incSupport() {
	support++;
    }

    public void clearSupport() {
	support = 0;
    }

    public int compareTo(Document other) {
	return this.support - other.support;
    }

    /*
     * append a word into the vector representation of the document
     * 
     * @param id id the of word(from the {@link Dictionary})
     * 
     * @param weight weight(e.g., tf*idf) of the word
     */
    public void append(int id, float weight) {
	vec.add(new Word(id, weight));
    }

    public void reOrder() {
	Collections.sort(vec);
    }

    public void normalize() {
	float sum = 0f;
	for (int i = 0; i < vec.size(); i++)
	    sum += vec.get(i).weight;

	for (int i = 0; i < vec.size(); i++)
	    vec.get(i).weight /= sum;
    }

    /*
     * calculate the similarity between this document and another document
     * 
     * @param other the other {@link Document} to compare
     * 
     * @return the similarity score
     */
    public float sim(Document other) {
	return cosine(this.vec, other.vec);
    }

    /*
     * the word id in each vector must be in order(desc or asce)
     */
    float cosine(Vector<Word> vec1, Vector<Word> vec2) {

	if (vec1.size() == 0 || vec2.size() == 0)
	    return 0f;

	int id1 = vec1.get(0).getId();
	int id2 = vec2.get(0).getId();
	float ret = 0f;
	int i = 0, j = 0;
	while (true) {
	    if (id1 == id2) {
		ret += vec1.get(i).getWeight() * vec2.get(j).getWeight();
		if (++i < vec1.size() && ++j < vec2.size()) {
		    id1 = vec1.get(i).getId();
		    id2 = vec2.get(j).getId();
		} else
		    break;
	    } else if (id1 < id2) {
		if (++i < vec1.size())
		    id1 = vec1.get(i).getId();
		else
		    break;
	    } else {
		if (++j < vec2.size())
		    id2 = vec2.get(j).getId();
		else
		    break;
	    }
	}

	return ret;
    }

    /*
     * encode the document(id, content, etc.) into a json string.
     * 
     * 
     * @return the encoded string
     */
    @SuppressWarnings("unchecked")
    public String toString() {
	JSONObject jsonTweet = new JSONObject();
	jsonTweet.put("id", new Long(id));

	jsonTweet.put("spt", new Integer(support));
	jsonTweet.put("time", new Long(time));

	JSONObject jsonVec = new JSONObject();
	for (int i = 0; i < vec.size(); i++)
	    jsonVec.put(vec.get(i).getId(), vec.get(i).getWeight());
	if (vec.size() > 0)
	    jsonTweet.put("vec", jsonVec);

	StringWriter out = new StringWriter();
	try {
	    jsonTweet.writeJSONString(out);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return out.toString();
    }

    /*
     * decode a document string
     */
    public static Document parse(String docStr) {
	try {
	    Document doc = new Document();
	    doc.decode(docStr);
	    return doc;
	} catch (Exception e) {
	    return null;
	}
    }

    public void decode(String docStr) {
	
	JSONObject obj = (JSONObject) JSONValue.parse(docStr);

	id = (Long) obj.get("id");
	time=(Long)obj.get("time");
	long spt = (Long) obj.get("spt");
	support=(int) spt;
	@SuppressWarnings("unchecked")
	HashMap<String, Double> vecObj = (HashMap<String, Double>) obj
		.get("vec");
	Vector<Word> vec = new Vector<Word>(vecObj.size());
	
	double w;
	for (Map.Entry<String, Double> entry : vecObj.entrySet()) {
	    w=(Double)entry.getValue();	    
	    vec.add(new Word(Integer.parseInt(entry.getKey()), (float)w));
	}
	
	this.vec=vec;
    }

    public class Word implements Comparable<Word> {
	int id;
	float weight;

	public Word(int id, float weight) {
	    this.id = id;
	    this.weight = weight;
	}

	public Word(Word other) {
	    this.id = other.id;
	    this.weight = other.weight;
	}

	public void setWeight(float w) {
	    weight = w;
	}

	public int getId() {
	    return id;
	}

	public float getWeight() {
	    return weight;
	}

	public int compareTo(Word other) {
	    return this.id - other.id;
	}
    }
}
