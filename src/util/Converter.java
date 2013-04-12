package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/*
 * read raw docs(e.g., tweets) from data collection(a set of files), then 
 * 1. extract key info, e.g., id, time, text, and remove unnecessary fields
 * 2. store the processed doc into a file according to its time window, 
 *    i.e., docs created in k-th time window would be stored in the k-th file
 * 3. construct a dictionary by parsing words from doc text
 */
public class Converter {
    /* files containing raw documents, each line in a file is one raw doc */
    public File rawDocsDir;

    /* encoded document are stored in this outFile, each line is a doc */
    public File docsDir;

    private Parser parser;
    private long progress;

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
	    System.out.println("deleting previous file:" + dictFile.getName());
	    //parser.loadDictionary();
	    dictFile.delete();
	}

	
	progress = 0;
    }

    /*
     * read raw documents(e.g., tweet) line by line, parse it by {@link Parser}.
     * store parsed docs on disk
     */
    public void convert() throws IOException {
	int k = 1;

	/*
	 * each time window has a file to store docs within that time period
	 * thus an entry in the hash map is time slot(window)-->BufferedWriter for a file
	 */
	HashMap<Integer, BufferedWriter> out = new HashMap<Integer, BufferedWriter>();
	for (File f : rawDocsDir.listFiles()) {
	    System.out
		    .println("Processing the:" + k + "th file:" + f.getName());

	    BufferedReader in = new BufferedReader(new FileReader(f));
	    doConvert(in, out);
	    in.close();
	}

	for (BufferedWriter writer : out.values()) {
	    writer.flush();
	    writer.close();
	}

	parser.saveDictionary();
	conf.saveStat(progress + " tweets have been processed");
    }

    void doConvert(BufferedReader in, HashMap<Integer, BufferedWriter> out)
	    throws IOException {
	String line;
	Document doc;
	String str;
	int slot;
	BufferedWriter writer;
	while ((line = in.readLine()) != null) {
	    doc = parser.parseRawDocument(line);

	    if (doc != null) {
		str = doc.encode() + "\n";
		slot = doc.getSlot();
		writer = out.get(slot);
		if (writer == null) {
		    System.out.println(docsDir.getPath());
		    writer = new BufferedWriter(new FileWriter(
			    docsDir.getPath()+"/"+ slot + ".txt"));
		    out.put(slot, writer);
		}

		writer.write(str);

		progress++;
		if (progress % 100 == 0)
		    System.out.println(progress + " docs have been processed");
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
