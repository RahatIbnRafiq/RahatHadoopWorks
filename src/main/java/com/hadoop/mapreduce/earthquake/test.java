package com.hadoop.mapreduce.earthquake;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;

public class test {
	public static void main(String[] args) {

        String csvFile = "F:\\My studies\\hadoop codes\\data_files\\earthquake.csv";
        BufferedReader br = null;
        String line = "";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
            	String [] tokens = line.split(",");
            	String location = tokens[tokens.length -1];
            	location = location.substring(1,location.length()-1);
            	String mag = tokens[tokens.length-4];
            	if(mag.equals("Magnitude"))
            		continue;
            	
               
                FloatWritable l = new FloatWritable();
                l.set(Float.parseFloat(mag));
                System.out.println(l);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
