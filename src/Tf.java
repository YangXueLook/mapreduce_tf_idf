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

public class Tf {

	// output format: key: cranNo + " " + term value: tf

	public static Hashtable<String, Integer> cranAndMaxfTable = new Hashtable<String, Integer>();

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text keyWord = new Text();
		Text fText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			String firstWord = itr.nextToken();

			if (firstWord.contains("cranfield") && firstWord.length() == 13)
			// format: cranfieldxxxx + " " + maxf
			{
				String maxfValueString = itr.nextToken();

				cranAndMaxfTable.put(firstWord,
						Integer.parseInt(maxfValueString));

			}

			else
			// format: keyword + " in " + cranfieldxxxx + f
			{
				itr.nextToken();
				String cranNo = itr.nextToken();
				// int maxf = cranAndMaxfTable.get(cranNo);

				String f = (itr.nextToken());
				// Double tf = (double) (f/maxf);

				

				keyWord.set(cranNo + " " + firstWord);
				fText.set(f);

				output.collect(keyWord, fText);

			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		Text keyWord = new Text();
		Text tfText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(key.toString());
			String cranNo = itr.nextToken();

			int maxf = cranAndMaxfTable.get(cranNo);
			while (values.hasNext()) {
				Text valueText = values.next();
				if (valueText.toString().equals("0.0")) {

				} else {



					Double tf = (Double.parseDouble(valueText.toString()) / ((double) maxf));
					// Double tf = (Double.parseDouble(valueText.toString()));

					tfText.set(tf.toString());

					output.collect(key, tfText);

				}

			}

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Tf.class);
		conf.setJobName("Tf");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);

		JobClient.runJob(conf);

	}
}
