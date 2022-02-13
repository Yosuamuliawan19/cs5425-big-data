// Matric Number:A0228565W
// Name:Yosua Muliawan
// WordCount.java
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopkCommonWords{

    public static class FileMapper extends  Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String stopwordsFile = "";
            Set<String> STOP_WORD_MAP = new HashSet<String>();

            // set the stop words
            String word = null;
            Configuration conf = context.getConfiguration();
            stopwordsFile = conf.get("stopwordsFile",  "");
            Path stopwordsFilePath = new Path(stopwordsFile);
            FileSystem fs = FileSystem.get(new Configuration());
            InputStreamReader is = new InputStreamReader(fs.open(stopwordsFilePath));
            BufferedReader br = new BufferedReader(is);

            while ((word = br.readLine()) != null) {
                STOP_WORD_MAP.add(word.toLowerCase()); //convert word from stopwatch file to lowercase
            }

            // add tokens
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = itr.nextToken();

                // split words based on delimiter
                String delimiters = " \n\r\t\f";
                StringTokenizer st = new StringTokenizer(word , delimiters);
                while (st.hasMoreTokens()) {
                    String cleanedWord = st.nextToken();

                    // check if contains stopwords
                    if (STOP_WORD_MAP.contains(cleanedWord.toString())){
                        continue;
                    }

                    Text currentText = new Text();
                    currentText.set(cleanedWord);
                    context.write(currentText, one);
                }
            }
        }
    }

    public static class FileReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CommonMapperFile1 extends  Mapper<Text, Text, Text, Text>{
        public void map(Text key, Text value, Context context ) throws IOException, InterruptedException {
            Text output  = new Text("f1-" + value.toString());
            context.write(key,  output);
        }
    }

    public static class CommonMapperFile2 extends  Mapper<Text, Text, Text, Text>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text output  = new Text("f2-" + value.toString());
            context.write(key,  output);
        }
    }

    public static class CommonReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Get values from all mappers. Decide which one is greater
            int size = 0;
            int maxValue = Integer.MIN_VALUE;
            for (Text val : values) {
                size += 1;
                int count = Integer.parseInt(val.toString().split("-")[1]);
                if (count > maxValue){
                    maxValue = count;
                }
            }
            // Write only if this word is used by more than one file
            if (size > 1){
                context.write(key, new  IntWritable(maxValue));
            }
        }
    }


    public static class SortMapper extends  Mapper<Text, Text, IntWritable, Text>{
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            int keyInt =-1 * Integer.parseInt(value.toString());
            context.write(new IntWritable(keyInt), key);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text >{
        private int max_count = 20;
        private int counter = 0;
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (counter >= max_count) return;

            // need to sort for lexicography
            List<String> words = new ArrayList<>();
            for (Text val : values) {
                words.add(val.toString());
            }
            Collections.sort(words, Collections.reverseOrder());

            // go through word list
            for(String word: words) {
                int keyInt = -1 * Integer.parseInt(key.toString());
                context.write(new IntWritable(keyInt), new Text(word));
                counter++;
                if(counter == max_count) break;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // parse args
        Configuration conf = new Configuration();
        String[] parsedArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (parsedArgs.length < 4) {
            System.out.println("Not enough arguments passed");
            return;
        }
        String inputFile1 = parsedArgs[0];
        String inputFile2 = parsedArgs[1];
        String stopwordsFile = parsedArgs[2];
        String outputFile = parsedArgs[3];
        String inputDir = new File(inputFile1).getParentFile().getName();
        String tempFile = inputDir + "/temp";

        conf.set("stopwordsFile", stopwordsFile);

        System.out.println("Parsed arguments");
        System.out.println(inputFile1);
        System.out.println(inputFile2);
        System.out.println(outputFile);
        System.out.println(tempFile);

        // word count for the first file
        Job job_File1 = Job.getInstance(conf, "word count file 1");
        job_File1.setJarByClass(TopkCommonWords.class);
        job_File1.setMapperClass(FileMapper.class);
        job_File1.setCombinerClass(FileReducer.class);
        job_File1.setReducerClass(FileReducer.class);
        job_File1.setOutputKeyClass(Text.class);
        job_File1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_File1, new Path(inputFile1));
        FileOutputFormat.setOutputPath(job_File1, new Path(tempFile + "/part1"));
        job_File1.waitForCompletion(true);
        System.out.println("File 1 completed");

        // word count for the second file
        Job job_File2 = Job.getInstance(conf, "word count file 2");
        job_File2.setJarByClass(TopkCommonWords.class);
        job_File2.setMapperClass(FileMapper.class);
        job_File2.setCombinerClass(FileReducer.class);
        job_File2.setReducerClass(FileReducer.class);
        job_File2.setOutputKeyClass(Text.class);
        job_File2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_File2, new Path(inputFile2));
        FileOutputFormat.setOutputPath(job_File2, new Path(tempFile + "/part2"));
        job_File2.waitForCompletion(true);
        System.out.println("File 2 completed");

        // count words in common, then sort
        Job job_CommonCount = Job.getInstance(conf, "word count common");
        job_CommonCount.setJarByClass(TopkCommonWords.class);
        job_CommonCount.setReducerClass(CommonReducer.class);


        job_CommonCount.setMapOutputKeyClass(Text.class);
        job_CommonCount.setMapOutputValueClass(Text.class);
        job_CommonCount.setOutputKeyClass(Text.class);
        job_CommonCount.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job_CommonCount, new Path(tempFile + "/part1"),
                KeyValueTextInputFormat.class, CommonMapperFile1.class);
        MultipleInputs.addInputPath(job_CommonCount, new Path(tempFile + "/part2"),
                KeyValueTextInputFormat.class, CommonMapperFile2.class);

        FileOutputFormat.setOutputPath(job_CommonCount, new Path(tempFile + "/common"));
        job_CommonCount.waitForCompletion(true);
        System.out.println("Common words job completed");

        // word count for the second file
        Job job_Sorter = Job.getInstance(conf, "sort");
        job_Sorter.setJarByClass(TopkCommonWords.class);
        job_Sorter.setInputFormatClass(KeyValueTextInputFormat.class);
        job_Sorter.setMapperClass(SortMapper.class);
        job_Sorter.setReducerClass(SortReducer.class);

        job_Sorter.setOutputKeyClass(IntWritable.class);
        job_Sorter.setOutputValueClass(Text.class);
        job_Sorter.setMapOutputKeyClass(IntWritable.class);
        job_Sorter.setMapOutputValueClass(Text.class); job_Sorter.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job_Sorter, new Path(tempFile + "/common"));
        FileOutputFormat.setOutputPath(job_Sorter, new Path(outputFile));

        job_Sorter.waitForCompletion(true);
        System.out.println("Sorting completed");

        System.exit(1);
    }
}
