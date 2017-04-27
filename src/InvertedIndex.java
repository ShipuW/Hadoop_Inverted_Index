import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Hashtable;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text loc = new Text();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	loc.set("");
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	if(loc.toString().equals("")){
    		loc.set(itr.nextToken());
    	}else{
    		word.set(itr.nextToken());
    		context.write(word, loc);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	String line = "";
    	Hashtable<String, Integer> table = new Hashtable<String, Integer>();
    	for (Text val : values) {
    		if(table.containsKey(val.toString())){
    			table.put(val.toString(), table.get(val.toString())+1);
    		}else{
    			table.put(val.toString(), 1);
    		}
    	}
    	Set<String> keys = table.keySet();
    	Iterator<String> itr = keys.iterator();
    	while (itr.hasNext()) {
        	String name = itr.next();
         	line = line + name + ":" + table.get(name) + "\t";
        } 
        result.set(line);
		context.write(key, result);
    }	
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}