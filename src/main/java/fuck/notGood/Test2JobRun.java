package fuck.notGood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test2JobRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		//下面两句话，可以设置从eclipse运行，注释掉这两句话则需要打包在linux下运行
		conf.set("fs.default.name", "hdfs://master:9000");
		conf.set("mapred.jar", "G:\\projects\\notGood\\target\\notGood-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
		try {
			Job job = new Job(conf);
			job.setJobName("QQ");
			job.setJarByClass(Test2JobRun.class);
			job.setMapperClass(Test2Mapper.class);
			job.setReducerClass(Test2Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
//			job.setNumReduceTasks(1);//设置reduce任务的个数
			//mapreduce 输入数据所在的目录或者文件
			FileInputFormat.addInputPath(job, new Path("/input/qq"));
			//mapreduce 执行之后的输出数据的目录
		    FileOutputFormat.setOutputPath(job, new Path("/output/qq"));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待job完成
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
