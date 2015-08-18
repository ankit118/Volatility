import java.io.IOException;
import java.util.Date;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class StockMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
			try {
				// Create a new Job
				long startTime = new Date().getTime();
			     Job jobA = Job.getInstance();
			     jobA.setJarByClass(StockMain.class);
				
				jobA.setMapperClass(VolatilityMapper.class);
				jobA.setReducerClass(VolatilityReducer.class);
				
				jobA.setOutputKeyClass(Text.class);
				jobA.setOutputValueClass(Text.class);
				
				jobA.setMapOutputKeyClass(Text.class);
				jobA.setMapOutputValueClass(Text.class);
				
				jobA.setInputFormatClass(TextInputFormat.class);
				jobA.setOutputFormatClass(TextOutputFormat.class);
				
				FileInputFormat.addInputPath(jobA, new Path(args[0]));
				FileOutputFormat.setOutputPath(jobA, new Path("output1"));
				
				jobA.waitForCompletion(true);
				
				Job jobB = Job.getInstance();
			     jobB.setJarByClass(StockMain.class);
				
				jobB.setMapperClass(StockMapper.class);
				jobB.setReducerClass(StockReducer.class);
				
				jobB.setOutputKeyClass(Text.class);
				jobB.setOutputValueClass(DoubleWritable.class);
				
				jobB.setMapOutputKeyClass(Text.class);
				jobB.setMapOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(jobB, new Path("output1"));
				FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
				
				boolean state = jobB.waitForCompletion(true);
				if(state){
					long endTime = new Date().getTime();
					System.out.println("Time taken" + (endTime-startTime));
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	
	}


