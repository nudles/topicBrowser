package util;

import java.util.Collections;
import java.util.Vector;


public class Document {
    public long id;
    
    protected long time;
    protected int support;
    protected int len;
    protected Vector<Word> vec;

    public Document(long id,int support,long time){
	this.id=id;
	this.support=support;
	this.time=time;
    }
    
    
    /*
     * increment the support of the document by one
     */
    public void incSupport(){
	support++;
    }
    
    public void clearSupport(){
	support=0;
    }
    
    /*
     * append a word into the vector representation of the document
     * 
     * @param id	id the of word(from the {@link Dictionary})
     * @param freq	the number of occurrence of the word in this document
     * 
     */
    public void append(int id, int freq){
	vec.add(new Word(id,freq));
    }
    
    /*
     * sort word in vector by id, and normalize it
     */
    public void normalize(){
	Collections.sort(vec);
	for(int i=0;i<vec.size();i++)
	    vec.get(i).weight/=len;
    }
    
    /*
     * return the slot of the document, which is calculated as (created_time-start_time)/time_window
     * 
     * @return slot of the document
     */
    public int getSlot(){
	return (int) ((time-Configure.startTime)/Configure.timeWindow);
    }
    
    /*
     * calculate the similarity between this document and another document
     * 
     * @param other	the other {@link Document} to compare
     * @return the similarity score
     */
    public float sim(Document other) {
	return cosine(this.vec, other.vec);
    }

    
    /*
     *the word id in each vector must be in order(desc or asce) 
     */
    float cosine(Vector<Word> vec1, Vector<Word> vec2) {
	int i = 0, j = 0;
	if (vec1.size() == 0 || vec2.size() == 0)
	    return 0f;

	int id1 = vec1.get(0).getId();
	int id2 = vec2.get(0).getId();
	float ret = 0f;

	while (i < vec1.size() && j < vec2.size()) {
	    if (id1 == id2)
		ret += vec1.get(i).getWeight() * vec2.get(j).getId();
	    else if (id1 < id2) {
		i++;
		id1 = vec1.get(i).getId();
	    } else {
		j++;
		id2 = vec2.get(j).getId();
	    }
	}

	return ret;
    }

    /*
     * encode the document(id, content, etc.) into a string for storing
     * this method must be implemented by child object
     * 
     * @return the encoded string
     */
    public String encode() throws UnimplementedException{
	throw new UnimplementedException("Document.encode() must be override!!\n");
    }

    /*
     * return the id of the document, e.g., tweet id
     * 
     * @return id of the document
     */
    public long getId() {
	return id;
    }

   

    protected class Word implements Comparable<Word> {
	int id;
	float weight;

	public Word(int id, float weight) {
	    this.id = id;
	    this.weight = weight;
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
