import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Idf {

	public static final int totalDocuments = 1400;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text keyWord = new Text();
		Text idf = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			// format: word + "  " +maxFre

			String keyString = itr.nextToken();
			
			
			String maxFre = itr.nextToken();
			
			

			Double idfValue = Math.log(totalDocuments
					/ Integer.parseInt(maxFre))
					/ Math.log(2);

			keyWord.set(keyString);
			idf.set(idfValue.toString());
			output.collect(keyWord, idf);

			

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			while (values.hasNext()) {
				
				
				
				
				output.collect(key,values.next());

			}
			

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Idf.class);
		conf.setJobName("Idf");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);

		JobClient.runJob(conf);

	}
}
