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

public class Weight {

	// output format: key: cranNo + " " + term value: tf

	public static Hashtable<String, Double> keywordAndIdfTable = new Hashtable<String, Double>();

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text keyWord = new Text();
		Text tfText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			String firstWord = itr.nextToken();

			if (firstWord.contains("cranfield") && firstWord.length() == 13)
			// format: cranfieldxxxx + keyword + tf
			{
				String keyWordString = itr.nextToken();
				String tf = itr.nextToken();
				
				//cranNo + keyword as combined key
				keyWord.set(firstWord + " " + keyWordString);
				
				tfText.set(tf);

				
				output.collect(keyWord, tfText);

			}

			else
			// format: keyword + " " + idf
			{
				
				String idfString = itr.nextToken();
				keywordAndIdfTable.put(firstWord, Double.parseDouble(idfString));

				

			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		Text keyWord = new Text();
		Text weightText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(key.toString());
			itr.nextToken();
			String keyWord = itr.nextToken();

			double idf = keywordAndIdfTable.get(keyWord);
			while (values.hasNext()) {

				Text tfValueText = values.next();
				if (tfValueText.toString().equals("0.0")) {

				} else {
					Double weight = idf * Double.parseDouble(tfValueText.toString());
					
					weightText.set(weight.toString());

					output.collect(key, weightText);
					
				}

			}

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Weight.class);
		conf.setJobName("Weight");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);

		JobClient.runJob(conf);

	}
}
