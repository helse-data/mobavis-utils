package com.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Christoffer Hjeltnes Støle
 */
public class GetConfig {
    
    Map <String, String> config = new HashMap();
    
    public GetConfig() {        
        try  {
            File file = new File(this.getClass().getResource("/config/config").toURI());
            
            InputStream inputStream = new FileInputStream(file.getPath());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);

            String line;

            while ((line = bufferedReader.readLine()) != null) {
                String[] splitLine = line.split(" ");
                //System.out.println("splitLine: " + Arrays.toString(splitLine));
                config.put(splitLine[0], splitLine[1]);
            }
            
            inputStream.close();
            reader.close();
            bufferedReader.close();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public String getConfig (String configuration) {
        return config.get(configuration);
    }
    
}
