
/*
 * WordCountMapper.java
 *
 * Created on Oct 22, 2012, 5:31:10 PM
 */

package org.sample;


import java.io.IOException;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author mac
 */
public class CompanyConnectionFirstMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountMapper");

    @Override
    protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
    	System.out.println("first mapper Key= "  + " values=>"+ value.toString()  );

		 String line[] = value.toString().split(":");
		 String fromCompany = line[0];
		 String toCompany[] = line[1].toString().split(",");
         for(int i =0 ; i <toCompany.length ; i++ ){
        	 
        	 String _key_toReducer = fromCompany + "#" + toCompany[i] + "#" + "false";
        	 context.write(new Text(_key_toReducer), new LongWritable(0));
        	 for(int j=0; j < toCompany.length ; j++){
        		 if(i ==j) continue;
        		 _key_toReducer = toCompany[i] + "#" + toCompany[j] + "#" + "true";
        		 context.write(new Text(_key_toReducer), new LongWritable(1));
        	 }
        	 
         }
    }
}
