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

public class MaxTermFrequency {
	public static Hashtable<String, Integer> wordAndMaxFrequencyTable = new Hashtable<String, Integer>();

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text cranfieldNo = new Text();
		Text content = new Text();

		String lowercase;

		Stemmer stemmer = new Stemmer();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			// format: word + " in " + cranNo + count

			String keyWord = itr.nextToken();
			itr.nextToken();
			String cranNo = itr.nextToken();
			int count = new Integer(itr.nextToken());

			if (wordAndMaxFrequencyTable.containsKey(cranNo)) {
				if (wordAndMaxFrequencyTable.get(cranNo) < count)
				// update max frequency
				{
					wordAndMaxFrequencyTable.remove(cranNo);
					wordAndMaxFrequencyTable.put(cranNo, count);
				}
			} else {
				wordAndMaxFrequencyTable.put(cranNo, count);
			}

			cranfieldNo.set(cranNo);
			content.set(wordAndMaxFrequencyTable.get(cranNo).toString());
			output.collect(cranfieldNo, content);

			
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text maxFrequency = new Text();
			
			Integer maxFre = 0;
			while (values.hasNext()) {
				Text value = values.next();
				if(Integer.parseInt(value.toString()) > maxFre)
				{
					maxFre = Integer.parseInt(value.toString());
				}
				
			}
			maxFrequency.set(maxFre.toString());
			output.collect(key, maxFrequency);
			
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MaxTermFrequency.class);
		conf.setJobName("MaxTermFrequency");
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
