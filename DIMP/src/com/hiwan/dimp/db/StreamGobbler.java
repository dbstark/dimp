package com.hiwan.dimp.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamGobbler extends Thread {  
  
    InputStream is;  
    String type;  
    String map;
  
    public StreamGobbler(InputStream is, String type,String map) {  
        this.is = is;  
        this.type = type;  
        this.map=map; 
    }  
  
    public void run() {  
        try {  
            InputStreamReader isr = new InputStreamReader(is);  
            BufferedReader br = new BufferedReader(isr);  
            String line = null;  
            while ((line = br.readLine()) != null) {  
                if (type.equals("Error")) {  
                    System.err.println("Error:" +map+ "   "+line);  
                } else {  
                    System.out.println("Debug:" +map+ "   "+line);  
                }  
            }  
        } catch (IOException ioe) {  
            ioe.printStackTrace();  
        }  
    }  
}  