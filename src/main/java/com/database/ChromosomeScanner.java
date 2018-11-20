package com.database;

import com.config.Config;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * 
 * Scans data files on the given chromosome.
 *
 * @author Christoffer Hjeltnes Støle
 */
public class ChromosomeScanner {
    String basePath;
    BufferedReader bufferedReader;
    String line = null;
    String[] splitLine = null;
    String thisSNPID = null;
    String previousSNPID;
    String chromosome;
    Config config = new Config();
    
    public ChromosomeScanner(String chromosome) {
        this.chromosome = chromosome;
        
        try {
            basePath = config.getConfig("path-server") + "data/snp_pheno_data/";            
        } catch (Exception e) {
            System.out.println(e);
        } 
        
        try {
            InputStream inputStream = new FileInputStream(basePath + "snp_pheno_" + chromosome + ".gz");
            InputStream gzipStream = new GZIPInputStream(inputStream);
            Reader reader = new InputStreamReader(gzipStream);
           
            bufferedReader = new BufferedReader(reader);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public String[] nextSNP() {
        StringBuilder dataString = new StringBuilder();
        
        try {
            if (line == null) {
                if (thisSNPID != null) { // we are at the end of the file
                    return null;
                }
                line = bufferedReader.readLine(); // read the header
                //System.out.println("header: " + line);
                
                line = bufferedReader.readLine(); // read the first non-header line
            }
            splitLine = line.split("\t");
            //System.out.println("line: " + line);
            previousSNPID = thisSNPID;
            while (line != null && (thisSNPID == null || previousSNPID == null|| thisSNPID.equals(previousSNPID))) {                
                previousSNPID = thisSNPID;
                //System.out.println(Arrays.toString(splitLine));
                dataString.append(splitLine[0]);
                for (int i = 1; i < splitLine.length; i++) {
                    if (i != 2) {
                        dataString.append("\t").append(splitLine[i]);
                    }                    
                }
                dataString.append("\n");
                line = bufferedReader.readLine(); // read the next line of the file
                //System.out.println("line: " + line);
                if (line != null) {
                    splitLine = line.split("\t"); // NULLPOINTER
                    //System.out.println(Arrays.toString(splitLine));
                    thisSNPID = splitLine[2];
                }
            }
            
        }        
        catch(IOException e) {
                System.out.println(e.getMessage());
        }
        return new String[] {previousSNPID, dataString.toString()};
    }
    
    
    public List <String> search(String pattern, boolean assumeContinuousData) {
        List <String> resultList = new ArrayList();
        System.out.println("Searching chromosome " + this.chromosome + " for pattern \"" + pattern +"\".");
        try {
            line = "";
            while (line != null) {
               line = bufferedReader.readLine();
               //System.out.println("line: " + line);
               if (line != null && line.contains(pattern)) {
                   System.out.println("Matching line: "  + line);
                   resultList.add(line);
               }
               else if (assumeContinuousData && resultList.size() > 0 ) {
                   break;
               }
            }
        }
        catch(IOException e) {
                System.out.println(e.getMessage());
        }
        return resultList;
    }
}
