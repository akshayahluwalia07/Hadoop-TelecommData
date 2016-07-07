import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class telecommreducer extends 
Reducer<Text, LongWritable, Text,LongWritable>

{
	LongWritable result=new LongWritable();
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
	throws IOException, InterruptedException
	{
		long sum=0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        this.result.set(sum);
        if (sum >= 60) {
            context.write(key, this.result);
        }
		
	}

}
