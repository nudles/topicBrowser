package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

public class Dictionary {

    // wordStr->word: map word string to Word instance
    private HashMap<String, Word> dict = null;

    // filter strange word string, e.g., @windy_linanda,KKKK
    private int occrThreshold = 1;

    File dictFile;
    Configure conf;

    public Dictionary(Configure conf) throws IOException {
	this.conf=conf;
	
	occrThreshold = conf.getWordOccrThreshold();
	dictFile = conf.getDictionaryFile();
	
	dict = new HashMap<String, Word>();
	
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

    public int retrieve(String w){
	Word word=dict.get(w);
	if(word!=null)
	    return word.id;
	else return -1;
    }
    void showStats() {

    }

    public void loadDictionary()throws IOException{
	loadDictionary(dictFile);
    }
    
    public void loadDictionary(File dictFile) throws IOException {
	System.out.println("loading dictionary from "+dictFile.getName());
	BufferedReader in = new BufferedReader(new FileReader(dictFile));
	String line;
	String word;
	int occrNum;

	while ((line = in.readLine()) != null) {
	    String[] field = line.split("\t");
	    
	    word = field[1];	   
	    occrNum = Integer.parseInt(field[2]);
	    if(occrNum>occrThreshold)
		add(word, occrNum);
	}
	
	System.out.println(dict.size()+" entities have been loaded\n");
    }

    public void saveDictionary() throws IOException {
	System.out.println("saving dictionary to disk...");
	BufferedWriter out = new BufferedWriter(new FileWriter(dictFile));

	for (Map.Entry<String, Word> entry : dict.entrySet()) {
	    save(entry.getValue(),out);
	}
	out.flush();
	out.close();
	System.out.println(dict.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");
	
	conf.saveStat(dict.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");
    }

    public void saveDictionarySortedByOccr() throws IOException {
	System.out.println("saving dictionary to disk...\n");
	BufferedWriter out = new BufferedWriter(new FileWriter(dictFile));

	List<Word> list = new ArrayList<Word>(dict.values());
	Collections.sort(list);

	for (Word w : list) {
	    save(w, out);
	}
	out.flush();
	out.close();
	System.out.println(list.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");
	
	conf.saveStat(dict.size() + " entities have been saved to "
		+ dictFile.getName()+"\n");
    }

    void save(Word w, BufferedWriter out) throws IOException {
	if (w.getOccr() > occrThreshold) {	    
	    out.write(w.getId() + "\t" + w.getWord() + "\t" + w.getOccr());
	    out.newLine();
	}
    }

    class Word implements Comparable<Word> {
	String raw;
	int id;
	int occr;

	public Word(String w, int id, int occr) {
	    this.raw = w;
	    this.id = id;
	    this.occr = occr;
	}

	String getWord() {
	    return raw;
	}

	int getId() {
	    return id;
	}

	int getOccr() {
	    return occr;
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
	d.loadDictionary(new File("data/bigDict.txt"));
	d.saveDictionarySortedByOccr();
    }
}
