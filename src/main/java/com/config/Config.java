package com.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Christoffer Hjeltnes Støle
 */
public class Config {
    
    Map <String, String> config = new HashMap();
    
    public Config() {        
        try  {
            File file = new File(this.getClass().getResource("/config/config").toURI());
            
            InputStream inputStream = new FileInputStream(file.getPath());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);

            String line;

            while ((line = bufferedReader.readLine()) != null) {
                String[] splitLine = line.split(" ");
                // code below supports system paths with spaces (but not configuration keys with spaces)
                config.put(splitLine[0], String.join(" ", Arrays.copyOfRange(splitLine, 1, splitLine.length))); 
            }
            
            inputStream.close();
            reader.close();
            bufferedReader.close();
        }
        catch (IOException | URISyntaxException e) {
            System.out.println(e);
        }
    }
    
    public String getConfig (String configuration) throws Exception {
        if (config.containsKey(configuration)) {
            return config.get(configuration);
        }
        else {
            throw new Exception("The required configuration key \"" + configuration + "\" was not found in the configuration file.");
        }
    }
    
}
