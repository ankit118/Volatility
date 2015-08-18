
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer extends Reducer<Text,Text,Text,DoubleWritable>{
	//String[] res; 
	//Map<String, Double> res = new TreeMap<String,Double>();
	ArrayList<String> res = new ArrayList<String>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(new Text("Bottom Stocks"), new DoubleWritable(10.0));
			for (Text val : values) {
				String[] words = val.toString().split("&");
				double d = Double.parseDouble(words[0]);
				DecimalFormat df = new DecimalFormat("##.######");
				String a = df.format(d);
				res.add(a+"&"+words[1]);
			}
			
			Collections.sort(res);
			int size = res.size();
			for(int i=0; i<10; i++){
				String[] words = res.get(i).split("&");
				context.write(new Text(words[1]), new DoubleWritable(Double.parseDouble(words[0])));
			}
			for(int j=0; j<10 ; j++){
				if(j==0){
					context.write(new Text("Top Stocks"), new DoubleWritable(10.0));
				}
				String[] words = res.get(size-(1+j)).split("&");
				context.write(new Text(words[1]), new DoubleWritable(Double.parseDouble(words[0])));
			}

	}
}