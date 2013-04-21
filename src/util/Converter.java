package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;


/*
 * read raw docs(e.g., tweets) from data collection(a set of files), then 
 * 1. extract key info, e.g., id, time, text, and remove unnecessary fields
 * 2. store the processed doc into a file according to its time window, 
 *    i.e., docs created in k-th time window would be stored in the k-th file
 * 3. construct a dictionary by parsing words from doc text
 */
public class Converter {
    static final Logger logger = Logger.getLogger(Converter.class);
    
    /* files containing raw documents, each line in a file is one raw doc */
    public File rawDocsDir;

    /* encoded document are stored in this outFile, each line is a doc */
    public File docsDir;

    private Parser parser;
  
    Configure conf = null;

    /*
     * @param conf {@link Configure} instance including inFile and outFile.
     * @param parser instance of child class of {@link Parser}
     */
    public Converter(Configure conf, Parser parser) throws IOException {
	this.conf = conf;
	this.parser = parser;
	
	rawDocsDir = conf.getRawDocsDir();
	docsDir = conf.getDocsDir();

	if (!rawDocsDir.isDirectory())
	    throw new IOException(
		    "A directory should be provided for raw docs\n");

	if (!docsDir.isDirectory()) {
	    throw new IOException(
		    "A directory should be provided to store processed docs\n");
	}

	File dictFile = conf.getDictionaryFile();
	if (dictFile.exists()) {
	    logger.info("deleting previous file:" + dictFile.getName());
	    //parser.loadDictionary();
	    dictFile.delete();
	}	
    }

    /*
     * read raw documents(e.g., tweet) line by line, parse it by {@link Parser}.
     * store parsed docs on disk
     */
    public void convert() throws IOException {
	
	/*
	 * each time window has a file to store docs within that time period
	 * thus an entry in the hash map is time slot(window)-->BufferedWriter for a file
	 */
	HashMap<Integer, BufferedWriter> out = new HashMap<Integer, BufferedWriter>();
	for (File f : rawDocsDir.listFiles()) {
	    logger.info("Processing file:" + f.getName());

	    BufferedReader in = new BufferedReader(new FileReader(f));
	    doConvert(in, out);
	    in.close();
	}

	for (BufferedWriter writer : out.values()) {
	    writer.flush();
	    writer.close();
	}

	parser.saveDictionary();	
    }

    void doConvert(BufferedReader in, HashMap<Integer, BufferedWriter> out)
	    throws IOException {
	
	int slot;
	String line;	
	String str;	
	Document doc;
	BufferedWriter writer;
	while ((line = in.readLine()) != null) {
	    doc = parser.parseRawDocument(line);

	    if (doc != null) {
		str = doc.toString() + "\n";
		slot = doc.getSlot();
		writer = out.get(slot);
		if (writer == null) {
		    
		    writer = new BufferedWriter(new FileWriter(
			    docsDir.getPath()+"/"+ slot + ".txt"));
		    out.put(slot, writer);
		}

		writer.write(str);		
	    }
	}
    }

    /*
     * save the dictionary constructed from the dataset to disk
     */
    public void saveDictionary() {
	parser.saveDictionary();
    }

}
