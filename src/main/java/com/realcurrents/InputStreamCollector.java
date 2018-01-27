package com.realcurrents;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Vector;

public class InputStreamCollector {
    final private Vector<InputStream> vs;
    final private int mark;
    
    public InputStreamCollector () {
        this.vs = new Vector<>();
        this.mark = 5251073;
    }
    
    public InputStreamCollector (int mark) {
        this.vs = new Vector<>();
        this.mark = mark;
    }
    
    public InputStreamCollector (Vector<InputStream> vs, int mark) {
        this.vs = vs;
        this.mark = mark;
    }
    
    public void collectInputStream (InputStream is) {
        this.vs.add(is);
    }
    
    public void collectInputStream (InputStream is, long byteSize) throws IOException {
        this.vs.add(is);
    }
    
    public InputStream getInputStream () {
        InputStream checkStream = new SequenceInputStream(this.vs.elements());
        checkStream.mark(this.mark);
        return checkStream;
    }
    
    public int getSize () throws IOException {
        InputStream checkStream = new SequenceInputStream(this.vs.elements());
        checkStream.mark(this.mark);
        try {
            System.out.println("InputStreamCollector vector size: " + this.vs.size());
            if (checkStream != null) {
                System.out.println("Available data: " + checkStream.available());
                return checkStream.available();
            }
        } catch (IOException e) {
            throw new IOException("Error checking available data for InputStreamCollector: "+ this +":\n"+ e.getMessage());
        }
        
        return this.vs.size();
    }
    
    public void clear () {
        this.vs.clear();
        System.gc();
    }
}
