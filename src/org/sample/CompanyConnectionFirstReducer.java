
/*
 * WordCountReducer.java
 *
 * Created on Oct 22, 2012, 5:33:40 PM
 */

package org.sample;


import java.io.IOException;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author mac
 */
public class CompanyConnectionFirstReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	
	String prevKey = null;
	Boolean samekey = false;
	
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                        throws IOException, InterruptedException {
    	
    	
    	String line[] = key.toString().split("#");
    	String currentKey = line[0]+"#"+line[1];

    	long sum = 0;
    	for (LongWritable _val : values) {
    		sum += _val.get();
    	}
     	//System.out.println("test Key " + key + "=>"+sum  );
     	
     	
     	
    	if(currentKey.equals(prevKey) && samekey == true){	
    	}else{
    		Boolean val = Boolean.valueOf(line[2]);
    		if(val){
    	     	
        		//System.out.println("write Key " + currentKey + "=>"+sum + val);

       		    context.write(new Text(currentKey), new LongWritable(sum));
    			samekey = false;
    		}else{
    			samekey = true;
    		}
    	}
    	prevKey = currentKey;		
    	
    	
    	
    	
    

      //	System.out.println("==================");
   }
    	
}
