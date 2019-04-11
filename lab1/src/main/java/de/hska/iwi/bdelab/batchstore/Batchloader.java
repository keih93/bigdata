package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import de.hska.iwi.bdelab.schema.Data;
import de.hska.iwi.bdelab.schema.DataUnit;
import de.hska.iwi.bdelab.schema.PageID;
import de.hska.iwi.bdelab.schema.PageViewEdge;
import de.hska.iwi.bdelab.schema.Pedigree;
import de.hska.iwi.bdelab.schema.UserID;
import manning.tap.DataPailStructure;

public class Batchloader {

    // ...

    private void readPageviewsAsStream(TypedRecordOutputStream out) {
        try {
            URI uri = Batchloader.class.getClassLoader().getResource("pageviews.txt").toURI();
            try (Stream<String> stream = Files.lines(Paths.get(uri))) {
                stream.forEach(line -> {
					try {
						writeToPail(getDatafromString(line), out);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
    }

    private Data getDatafromString(String pageview) {

        StringTokenizer tokenizer = new StringTokenizer(pageview);
        String ip = tokenizer.nextToken();
        String url = tokenizer.nextToken();
        String time = tokenizer.nextToken();

        System.out.println(ip + "| " + url + "| " + time);
        // ... create Data
        UserID user = new UserID();
        user.set_user_id(ip);
        
        PageID page = new PageID();
        page.set_url(url);
        
        Random rand = new Random();

        int nonce = rand.nextInt();
        System.out.println("Nonce is" + nonce);
        PageViewEdge view = new PageViewEdge(user, page, nonce);
        
        DataUnit du = new DataUnit();
        du.set_view(view);

        Integer predi = Integer.parseInt(time);        
        Pedigree pre = new Pedigree(predi);   
        Data result = new Data(pre, du);
        
        return result;
    }

    private void writeToPail(Data data, TypedRecordOutputStream out) throws IOException {
			out.writeObjects(data);
    }

    private void importPageviews() {

        // change this to "true" if you want to work
        // on the local machines' file system instead of hdfs
        boolean LOCAL = false;

        try {
            // set up filesystem
            FileSystem fs = FileUtils.getFs(LOCAL);

            // prepare temporary pail folder
            String newPath = FileUtils.prepareNewFactsPath(true, LOCAL);

            // master pail goes to permanent fact store
            String masterPath = FileUtils.prepareMasterFactsPath(true, LOCAL);

            // set up new pail and a stream
            Pail source = Pail.create(fs, newPath, new DataPailStructure());
            Pail target = Pail.create(fs, masterPath, new DataPailStructure());
            
            // write facts to new pail
            TypedRecordOutputStream out = source.openWrite();
            readPageviewsAsStream(out);
	    	out.close();
	    	
            // set up master pail and absorb new pail
	    	target.absorb(source);
            target.consolidate();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Batchloader loader = new Batchloader();
        loader.importPageviews();
    }
}