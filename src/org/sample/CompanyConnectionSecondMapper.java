
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
public class CompanyConnectionSecondMapper extends Mapper<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountMapper");

    @Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException {
	
    	Long  lineValue = Long.parseLong(value.toString());
        String line2[] = key.toString().split("#");
    	//System.out.println("First Mapper Key= "+ line2[0] + "-"+ line2[1] + " values=>"+  lineValue  );
    	context.write(new Text(line2[0]), new Text(lineValue+ ":"+ line2[1]));
    }
}
