package org.conan.myhadoop.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;
/**
 * PR标准化
 * @author Administrator
 *
 */
public class Normal {

    public static class NormalMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text("1");

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            System.out.println(values.toString());
            context.write(k, values);
        }
    }

    public static class NormalReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> vList = new ArrayList<String>();

            float sum = 0f;
            for (Text line : values) {
                vList.add(line.toString());

                String[] vals = PageRankJob.DELIMITER.split(line.toString());
                float f = Float.parseFloat(vals[1]);
                sum += f;
            }
            
            Map<Float, Float> mymap = new HashMap<Float, Float>();
            for (String line : vList) {
                String[] vals = PageRankJob.DELIMITER.split(line.toString());
                Text k = new Text(vals[0]);
                
                float f = Float.parseFloat(vals[1]);
                Text v = new Text(PageRankJob.scaleFloat((float) (f / sum)));
                context.write(k, v);
                
//                System.out.println(k + "::" + v);//打印最后结果
                float kk = Float.parseFloat(k.toString());
                float vv = Float.parseFloat(v.toString());
                
                mymap.put(kk, vv);
            }
            List<Map.Entry<Float, Float>> infoIds = new ArrayList<Map.Entry<Float, Float>>( mymap.entrySet()); 
    		//排序前 
    		for (int i = 0; i < infoIds.size(); i++) { 
    		String id = infoIds.get(i).toString(); 
    		System.out.println(id); 
    		} 
    		//排序 
    		Collections.sort(infoIds, new Comparator<Map.Entry<Float, Float>>() { 
    			public int compare(Map.Entry<Float, Float> o1, Map.Entry<Float, Float> o2) 
    			{ 
    				return (int) (o2.getValue()*1000000000 - o1.getValue()*1000000000); 
    			} 
    		}); 
    		
    		System.out.println("================================");
    		//排序后 
    		for (int i = 0; i < infoIds.size(); i++) { 
    		String id = infoIds.get(i).toString(); 
    		System.out.println(id); 
    		} 
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = PageRankJob.config();

        String input = path.get("input_pr");
        String output = path.get("result");

        HdfsDAO hdfs = new HdfsDAO(PageRankJob.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Normal.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(NormalMapper.class);
        job.setReducerClass(NormalReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

    }

}
