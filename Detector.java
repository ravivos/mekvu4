import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.FileSystem;

/**
 * the class of the Detector
 * 
 * @author Raviv Rachmiel <raviv.rachmiel@gmail.com>
 * @since Jan 26, 2017
 */
public class Detector {
	// User local temp folder
	private static final Path TEMP_PATH = new Path("temp");
	private static final Path TEMP_PATH2 = new Path("temp2");

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());
			while (st.hasMoreTokens()) {
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				word.set(fileName + " " + st.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(-sum);
			context.write(key, result);
		}
	}
	
	
	public static class SwapMapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			context.write(value, key);
		}
	}

	public static class OutputReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}

	/*
	 * filename word -> count -> filename -> word,count (sorted)
	 */
	public static class FileToResMapper extends Mapper<Text, Text, Text, Text> {
		private final Text word = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());
			while (st.hasMoreTokens()) {
				String temp = key.toString();
				String nt = st.nextToken();
				word.set(temp.split(" ")[0]); // the file name
				context.write(word, new Text(temp.split(" ")[1]+ " " + nt));
			}
		}
	}

	public static class CuttingReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			int n = Integer.parseInt(configuration.get("n"));
			String s = "";
			int count = 0;
			for (Text val : values) {
				count++;
				s += val.toString() + " ";
				if (count == n)
					break;
			}
			context.write(key, new Text(s));
		}
	}
	// END

	/*
	 * TODO: we have text1.txt topN text2.txt topN text3.txt topN text4.txt topN
	 * text5.txt topN ... we need text1.txt text2.txt sum(inter(topN1,topN2))
	 * 
	 * ->
	 * 
	 * word -> filename,count -> reduce to: <filename 1,filename2> -> count1 +
	 * count2
	 */
	public static class WordToFileCount extends Mapper<Text, Text, Text, Text> {
		private final Text word = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());
			while (st.hasMoreTokens()) {
				String temp = key.toString();
				String nt = st.nextToken();
				word.set(nt.split(" ")[0]); // word
				context.write(word, new Text(temp + " " + nt.split(" ")[1]));
			}
		}
	}

	/*
	 * f:(ngram, list<fileName,sum>)->list<(file1, file2), totSum> for example:
	 * "hash little baby" list=( (summertime.txt, 2), (lullaby.txt, 1) ) output:
	 * (summertime.txt, lullaby.txt, 3)
	 */
	public static class ToTupsReducer extends Reducer<Text, Text, Text, IntWritable> {
		Queue<String> queue = new LinkedList<String>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				queue.add(val.toString());
			}
			String[] valuesArray = new String[queue.size()];
			int idx = 0;
			while (!queue.isEmpty()) {
				valuesArray[idx] = queue.poll();
				idx++;
			}
			for (int i = 0; i < valuesArray.length; i++) {
				for (int j = i + 1; j < valuesArray.length; j++) {
					int sum = Integer.parseInt(valuesArray[i].split(" ")[1]) + Integer.parseInt(valuesArray[j].split(" ")[1]);
					context.write(new Text(valuesArray[i].split(" ")[0] +" " + valuesArray[j].split(" ")[0]),
							new IntWritable(sum));
				}
			}
		}
	}

	 /*
     * Last one
     */
    /*
     * f:(file1 file 2 sum)->(<file1, file2>, sum)
     */
    public static class AllPairsSummer extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s[] = value.toString().split(" ");
            Text fileName1 = new Text(s[0]);
            Text fileName2 = new Text(s[1]);
            int count = Integer.parseInt(s[2]); 
            Text p = new Text(fileName1 + ", " + fileName2);
            context.write(p, new IntWritable(count));
        }
         
    }
     
    /*
     * f:(<file1, file2>, list(sum))-> <file1, file2>
     * if H(file1, file2)>=k
     */
    public static class ReduceSumK extends Reducer <Text, IntWritable, Text, IntWritable>{
             
        public void reduce(Text t, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            int k = Integer.parseInt(configuration.get("k"));
             
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            sum = -sum;
            if (sum >= k){
                context.write(t, new IntWritable(sum));
            }
        }
    }
	
	


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		conf.set("n", args[0]);
		conf.set("k", args[1]);

		// Just to be safe: clean temporary files before we begin
		fs.delete(TEMP_PATH, true);

		/*
		 * We chain the two Mapreduce phases using a temporary directory from
		 * which the first phase writes to, and the second reads from
		 */

		// Setup first MapReduce phase
		// this MapReduce will take all the files and make a map of word to
		// counter

		/*
		 * gets some files and maps file and word to occurence num
		 */
		System.out.println("Hello");
		Job job1 = Job.getInstance(conf, "Detector-first");
		job1.setJarByClass(Detector.class);
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, TEMP_PATH);
		// FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.out.println("Hello1");
		boolean status1 = job1.waitForCompletion(true);
		if (!status1) {
			System.exit(1);
		}

		/*
		 * /* map file to tuple of word and occurences
		 * 
		 */
		// Setup second MapReduce phase

		System.out.println("Hello2");
		Job job3 = Job.getInstance(conf, "Detector-second");
		job3.setJarByClass(Detector.class);
		job3.setMapperClass(SwapMapper.class);
		job3.setReducerClass(OutputReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job3, TEMP_PATH);
		FileOutputFormat.setOutputPath(job3, TEMP_PATH2);
		// FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		System.out.println("Hello3");
		boolean status3 = job3.waitForCompletion(true);
		System.out.println("Hello4");

		if (!status3)
			System.exit(1);

		/*
		 * maps file to list of words and occurences reduce to only first N
		 * (which will be sorted because of the prev map red
		 */
		// Setup second MapReduce phase

		System.out.println("Hello5");
		Job job2 = Job.getInstance(conf, "Detector-second");
		job2.setJarByClass(Detector.class);
		job2.setMapperClass(FileToResMapper.class);
		job2.setReducerClass(CuttingReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, TEMP_PATH2);
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		System.out.println("Hello6");
		boolean status2 = job2.waitForCompletion(true);
		System.out.println("Hello7");

		if (!status2)
			System.exit(1);

		/*
		 * do cartesian on the prev map reduce keys and intersect the vals
		 * remove (b,a) if we have (a,b)
		 * 
		 */

		fs.delete(TEMP_PATH, true);
		fs.delete(TEMP_PATH2, true);

	}
	// Clean temporary files from the first MapReduce phase

}
