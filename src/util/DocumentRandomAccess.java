package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class DocumentRandomAccess {

    // map doc id to its offset in doc file which has the raw text
    private HashMap<Long, Long> docOffsetMap;

    // file storing documents
    RandomAccessFile file;

    public DocumentRandomAccess(String path) throws NumberFormatException, IOException{
	docOffsetMap = new HashMap<Long, Long>(100000);
	loadOffset(path);
    }
    
    public DocumentRandomAccess() {
	docOffsetMap = new HashMap<Long, Long>(100000);
    }

    public void addOffset(long id, long offset) {
	docOffsetMap.put(id, offset);
    }

    public boolean startRead(String path) throws FileNotFoundException {
	file = new RandomAccessFile(path+".txt", "r");
	return file != null;
    }

    public String readDocument(long id) {
	Long offset = docOffsetMap.get(id);
	String ret=null;
	if (offset != null) {
	    try {
		file.seek(offset);
		ret=file.readLine();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	
	return ret;
    }

    public void endRead() {
	try {
	    file.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    
    
    /*
     * save the offset records into file
     * 
     * @param path the file for storing the offset records
     */
    public void saveOffset(String path) throws IOException{
	BufferedWriter out=new BufferedWriter(new FileWriter(path+".offset"));
	for(Map.Entry<Long, Long>entry:docOffsetMap.entrySet()){
	    out.write(entry.getKey()+","+entry.getValue());
	    out.newLine();
	}
	out.flush();
	out.close();
    }
    
    /*
     * read offset records from file
     * 
     * @param path file storing the records
     */
    public void loadOffset(String path) throws NumberFormatException, IOException{
	BufferedReader in=new BufferedReader(new FileReader(path+".offset"));	
	
	String line;
	while((line=in.readLine())!=null){
	    int pos=line.indexOf(",");
	    long id=Long.parseLong(line.substring(0,pos));
	    long offset=Long.parseLong(line.substring(pos+1));
	    docOffsetMap.put(id,offset);
	}
	
	in.close();
    }

}
