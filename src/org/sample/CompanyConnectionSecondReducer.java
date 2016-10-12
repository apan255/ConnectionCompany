
/*
 * WordCountReducer.java
 *
 * Created on Oct 22, 2012, 5:33:40 PM
 */

package org.sample;
import java.util.PriorityQueue;


import java.awt.List;
import java.io.IOException;



import java.util.ArrayList;



import java.util.Comparator;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author mac
 */
public class CompanyConnectionSecondReducer extends Reducer<Text,Text,Text,Text> {
	
	String prevKey = null;
	Boolean samekey = false;
	ArrayList<String> storeArray = new ArrayList<String>();
	
	public static class maxCalculation  {
		 
		public String companySecond;
		public int value;
		
		maxCalculation(String company, int _value){
			companySecond = company;
			value = _value;
			
		}
	}
	
	
 
	//PriorityQueue<maxCalculation> pq1 = new PriorityQueue<maxCalculation>();
	
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
    	
    	//Comparator<maxCalculation> queueComparator = new maxCalculation(prevKey, 0);
		//PriorityQueue<String> priorityQueue = new PriorityQueue<String>(10,
		//		queueComparator);
    	//PriorityQueue<maxCalculation> pq1 = new PriorityQueue<maxCalculation>();

    	PriorityQueue<maxCalculation> pq1 = new PriorityQueue<maxCalculation>(10, new Comparator<maxCalculation>(){

    		@Override
    		public int compare(maxCalculation ab, maxCalculation cd){
    		            if(ab.value<cd.value)
    		            {
    		                return 1;

    		            }
    		            return 0;
    		}
    		});
    	
    	for (Text _val : values) {
	    	String line[] = _val.toString().split(":");
			//String _nety = line[0] + ":" + line[1];
			maxCalculation _maxCalculation= new maxCalculation(line[1],Integer.parseInt(line[0]));
			pq1.add(_maxCalculation);
    	}
    
		 if(!pq1.isEmpty()){
			 
			 String __value = "\t\t\tRecommend\t";
 			for(int _count =1; _count <=2 && !pq1.isEmpty(); _count++ ){
 				
 				maxCalculation maCalculation = pq1.poll();
 				__value = "[" + maCalculation.companySecond +":"+ maCalculation.value + "]," +  __value;
 		   }
		 
  		    System.out.println("Writing Second  reducer currentKey = "+  key.toString()  +" val" + __value);
		    context.write(new Text(__value), new Text("To =>" + key.toString() ));
		 }
   }
    	
}
