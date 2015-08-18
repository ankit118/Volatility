import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;

public class VolatilityMapper extends Mapper<Object, Text, Text, Text>{
	
	int counter = 0;
	private Text file = new Text();
	private Text datePrice = new Text();
	private String fileName = "";
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if(value.toString().startsWith("Date") || counter==0) { 
			counter++;
			FileSplit i = (FileSplit) context.getInputSplit();
			fileName = i.getPath().getName();
			String [] stockName = fileName.split("\\.");
			fileName = stockName[0];
			return;
		} else { 
				String [] words = value.toString().split(",");
				if(words != null && words.length>0) {
					int length = words.length;
					String date = words[0];
					String price = words[length-1];
					date = date + "&" + price;
					if(date != null && date.length() >0)
						file.set(fileName);
						datePrice.set(date);
						context.write(file,datePrice);
						counter++;
				}
		}
	}
}