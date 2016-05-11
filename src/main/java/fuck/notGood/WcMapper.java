package fuck.notGood;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	//每次调用map方法，会传入split中的一行数据key:该行数据所在文件中的位置下标，value:该行数据
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);//默认按照空格切分
		while(st.hasMoreTokens()){
			String world = st.nextToken();
			context.write(new Text(world), new IntWritable(1));//map的输出
		}
	}
}
