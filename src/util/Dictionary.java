package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

public class Dictionary {
    static final Logger logger = Logger.getLogger(Dictionary.class);
    // wordStr->word: map word string to Word instance
    public HashMap<String, Word> dict = null;

    // filter strange word string, e.g., @windy_linanda,KKKK
    private int occrThreshold = 1;
    
    float TotalDocs=10000000f;

    File dictFile;
    Configure conf;

    public Dictionary(Configure conf) throws IOException {
	this.conf=conf;
	
	occrThreshold = conf.getWordOccrThreshold();
	dictFile = conf.getDictionaryFile();
	
	dict = new HashMap<String, Word>();
	TotalDocs=conf.getTotalDocN();
	
    }

    public int add(String w) {
	return add(w, 1);
    }

    int add(String w, int occr) {
	Word word = dict.get(w);
	if (word != null)
	    word.occr += occr;
	else {
	    word = new Word(w, dict.size() + 1, occr);
	    dict.put(w, word);
	}

	return word.getId();
    }

    public Word retrieve(String w){	
	return dict.get(w);
    }
    
    
    
    void showStats() {

    }
    
    public float getIdf(int id){
	return (float) Math.log(dict.size()/(1.0f+dict.get(id).getOccr()));
    }
    
    /*
     * @return number of enties of the dictionary
     */
    public int getSize(){
	return dict.size();
    }

    public void loadDictionary()throws IOException{
	loadDictionary(dictFile);
    }
    
    public void loadDictionary(File dictFile) throws IOException {
	logger.info("loading dictionary from "+dictFile.getName());
	BufferedReader in = new BufferedReader(new FileReader(dictFile));
	String line;
	String word;
	int occrNum;
	int wid;

	while ((line = in.readLine()) != null) {	    
	    String[] field = line.split("\t");
	    
	    wid=Integer.parseInt(field[0]);
	    word = field[1];	   	    
	    occrNum = Integer.parseInt(field[2]);
	    if(occrNum>occrThreshold)
		dict.put(word, new Word(wid,(float) (Math.log(TotalDocs/(1f+occrNum)))));	    
	}
	in.close();
	
	logger.info(dict.size()+" entities have been loaded\n");
    }
    
    /*
     * load the dictionary for adjustment
     */
    public void loadRawDictionary(File dictFile) throws IOException {
	logger.info("loading dictionary from "+dictFile.getName());
	BufferedReader in = new BufferedReader(new FileReader(dictFile));
	String line;
	String word;
	int occrNum;
	int wid;

	while ((line = in.readLine()) != null) {	    
	    String[] field = line.split("\t");
	    
	    wid=Integer.parseInt(field[0]);
	    word = field[1];	   	    
	    occrNum = Integer.parseInt(field[2]);
	    if(occrNum>occrThreshold)
	    {
		dict.put(word, new Word(word,wid,occrNum));//Word(word,wid,occrNum));
	    }
	}
	in.close();
	logger.info(dict.size()+" entities have been loaded\n");
    }

    public void saveDictionary() throws IOException {
	logger.info("saving dictionary to disk...");
	BufferedWriter out = new BufferedWriter(new FileWriter(dictFile));

	for (Map.Entry<String, Word> entry : dict.entrySet()) {
	    save(entry.getValue(),out);
	}
	out.flush();
	out.close();
	logger.info(dict.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");	
    }

    public void saveDictionarySortedByOccr() throws IOException {
	logger.info("saving dictionary to disk...");
	BufferedWriter out = new BufferedWriter(new FileWriter(dictFile));

	List<Word> list = new ArrayList<Word>(dict.values());
	Collections.sort(list);

	for (Word w : list) {
	    save(w, out);
	}
	out.flush();
	out.close();
	logger.info(list.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");
	
    }

    void save(Word w, BufferedWriter out) throws IOException {
	if (w.getOccr() > occrThreshold)
	{	    
	    out.write(w.getId() + "\t" + w.getWord() + "\t" + w.getOccr());
	    out.newLine();
	}
    }

    public class Word implements Comparable<Word> {
	String raw;
	int id;
	int occr;
	float idf;

	public Word(String w, int id, int occr) {
	    this.raw = w;
	    this.id = id;
	    this.occr = occr;
	}
	
	public Word(int id, float idf) {
	    
	    this.id = id;
	    this.idf=idf;
	}

	String getWord() {
	    return raw;
	}

	public int getId() {
	    return id;
	}

	public int getOccr() {
	    return occr;
	}
	
	public float getIdf(){
	    return idf;
	}

	public int compareTo(Word other) {
	    return this.occr - other.occr;
	}
    }

    // test functions of class Dictionary
    public static void main(String[] args) throws ParserConfigurationException,
	    SAXException, IOException {
	Configure conf = new Configure("config/config.xml");
	conf.config();

	Dictionary d = new Dictionary(conf);
	d.loadRawDictionary(new File("data/dictionary.txt"));
	d.saveDictionarySortedByOccr();
    }
}
