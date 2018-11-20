package com.database;

import com.config.Config;
import com.parallel.AgeUtils;
import com.parallel.ConstantsUtils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksIterator;

/**
 * 
 * Creates databases with RocksDB for genotyping data from MoBa.
 * 
 * @author Christoffer Hjeltnes Støle
 */
public class CreateDatabase {
    ConstantsUtils constants = new ConstantsUtils();
    String[] chromosomes = constants.getChromosomeList();
    String sharedPath;
    Config config = new Config();
    
    public CreateDatabase () {
        try {
            sharedPath = config.getConfig("path-server") + "/data/";            
        } catch (Exception e) {
            System.out.println(e);
        }        
    }
    
    public void CreateSummaryStatisticsDatabase() {
        AgeUtils ageUtils = new AgeUtils();
        
        
        String columnSeparator = "\t";
        
        try {
            InputStream inputStream = new FileInputStream(sharedPath + "summary_statistics/population_conditioned.txt");
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String[] headers = bufferedReader.readLine().split(columnSeparator);
            String line;
            String phenotype = "";
            String age = "";
            boolean isLongitudinal;
            
            String getPhenotype = "fatherBmi";
            String data = "";
            List <String> dataList = new ArrayList();

            while ((line = bufferedReader.readLine()) != null) {
                //System.out.println(line);
                String[] splitLine = line.split(columnSeparator);
                phenotype = splitLine[0];
                String sex = splitLine[1];
                String conditionPhenotype = splitLine[2];
                String conditionPercentile = splitLine[3];
                
                
//                if (phenotype.equals(getPhenotype)) {
//                    //data += line + "\n";
//                    dataList.add(line);
//                }

                if (phenotype.startsWith("weight")) {
                    //data += line + "\n";
                    dataList.add(line);
                }
                
                //String percentile = null;
                //age = ageUtils.getAgeString(phenotype);
                //System.out.println("phenotype: " + phenotype);
//                if (age != null) {// longitudinal phenotype
//                    isLongitudinal = true;
//                    percentile = splitLine[5]; 
//                    if (age.equals("birth")) { 
//                        phenotype = phenotype.replace("Birth", "");
//                    }
//                }
            }

            //System.out.println("data: " + dataList);
            System.out.println("File browsed.");
            StringBuilder dataString = new StringBuilder("");
            for (String dataLine : dataList) {
                dataString.append(dataLine).append("\n");
            }
            
            System.out.println("data string: ");// + dataString);
            
            inputStream.close();
            reader.close();
            bufferedReader.close();
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void CreateSNPDatabase() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setErrorIfExists(true);
        
        chromosomes = new String[] {"7"};
        
        try {
            for (String chromosome : chromosomes) {
//                if (!(chromosome.equals("mockup") || chromosome.equals("22"))) {
//                    options.setErrorIfExists(true);
//                }
//                else {
//                    options.setErrorIfExists(false);
//                }
                System.out.println("Creating database for chromosome " + chromosome + ".");
                String databaseName = chromosome + "_new";
                String path = sharedPath + "databases/" + databaseName ;
                System.out.println("Database name: " + databaseName);
                RocksDB db = RocksDB.open(options, path);
                ChromosomeScanner chromosomeScanner = new ChromosomeScanner(chromosome);

                String[] res;
                String snpID = null;
                
                while ((res = chromosomeScanner.nextSNP()) != null) {
                    //System.out.println(Arrays.toString(res));
                    //System.out.println("Beginning and end: " + res[1].substring(0, 25) + " ... " + res[1].substring(res[1].length()-25, res[1].length()));
                    //System.out.println("SNP ID: " + res[0]);
                    snpID = res[0];
                    
                    db.put(snpID.getBytes(), res[1].getBytes());
                    //System.exit(0);
                }
                System.out.println("Database created for chromosome " + chromosome + ".");
                System.out.println("last SNP ID: " + snpID);
                
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }        
    }
    
    public void CreateSubSNPDatabase(List <String> SNPSelection, String chromosome, String dbName) {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setErrorIfExists(true);
        
        try {
            System.out.println("Creating sub database for chromosome " + chromosome + ".");
            //String databaseName = chromosome + "_sub_new";
            String path = sharedPath + "databases/" + dbName ;
            System.out.println("Database name: " + dbName);
            RocksDB db = RocksDB.open(options, path);
            ChromosomeScanner chromosomeScanner = new ChromosomeScanner(chromosome);

            String[] res;
            String snpID = null;

            int found = 0;
            while ((res = chromosomeScanner.nextSNP()) != null) {
                //System.out.println(Arrays.toString(res));
                //System.out.println("Beginning and end: " + res[1].substring(0, 25) + " ... " + res[1].substring(res[1].length()-25, res[1].length()));
                //System.out.println("SNP ID: " + res[0]);
                snpID = res[0];
                if (SNPSelection.contains(snpID)) {
                    found++;
                    db.put(snpID.getBytes(), res[1].getBytes());
                    if (found == SNPSelection.size()) {
                        System.out.println("All SNPs found.");
                        break;
                    }
                }                
                //System.exit(0);
            }
            System.out.println("Database created for chromosome " + chromosome + ".");
            System.out.println("last SNP ID: " + snpID);
            System.out.println("Estimated of number of keys: " + db.getProperty("rocksdb.estimate-num-keys"));
            
            ReadDatabase databaseReader = new ReadDatabase();
            
            int maxIterSize = 1000;
            if (SNPSelection.size() <= maxIterSize) {
                databaseReader.iterateAllKeys(db);
            }
            else {
                System.out.println("Skipped key iteration for large database (more than " + maxIterSize + ") entries");
            }
            
            db.close();
            
        }
        catch (Exception e) {
            System.out.println(e);
        }        
    }
    
}
