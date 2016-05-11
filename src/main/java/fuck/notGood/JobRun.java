package fuck.notGood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobRun.class);
			job.setMapperClass(WcMapper.class);
			job.setReducerClass(WcReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
//			job.setNumReduceTasks(1);//设置reduce任务的个数
			//mapreduce 输入数据所在的目录或者文件
			FileInputFormat.addInputPath(job, new Path("/input/wc"));
			//mapreduce 执行之后的输出数据的目录
		    FileOutputFormat.setOutputPath(job, new Path("/output/wc"));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待job完成
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
