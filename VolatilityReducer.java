
import java.io.IOException;

import java.util.LinkedList;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class VolatilityReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String prevMonth = "";
	double inprice = 0d;
	double prevprice = 0d;
	double sum = 0d; 
	double ror = 0d;
	int month = 0;
	double volatility = 0d;
	
	LinkedList<Double> ror_List = new LinkedList<Double>(); 
	for (Text val : values) {
	String [] words = val.toString().split("&");
	if (words.length < 2) { 
	    continue;
	}
	String date = words[0];
	String price1 = words[1];
	date = date.substring(5,7);
	double price = Double.parseDouble(price1);
	if (inprice == 0 && prevMonth.equals("")) {
		inprice = price;
		prevMonth = date;
	}
	if (!date.equals(prevMonth) && prevprice != 0) {
		month += 1;
		if(inprice != 0) ror_List.add((prevprice - inprice)/inprice);
	    inprice = price;
	}
	prevMonth = date;
	prevprice = price;

	}
	month += 1;
	if(inprice != 0) ror_List.add((prevprice - inprice)/inprice);
	sum = 0;
	       for (int i =0; i<ror_List.size(); i++) {
	           sum += ror_List.get(i);
	       }
	       
	sum = sum/month;
	Double temp =0d;
	for(int i=0; i<ror_List.size(); i++){
	temp += Math.pow(ror_List.get(i) - sum, 2);  
	}
	if(month>1){
	if(temp>0) volatility = Math.sqrt(temp/(month-1));
	if(volatility != 0) {
		context.write(key,new Text(volatility + ""));
	}
	}
	}
}