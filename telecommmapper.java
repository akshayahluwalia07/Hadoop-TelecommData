import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class telecommmapper extends Mapper<LongWritable, Text, Text, LongWritable>
{

	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
	
	String[] splits = value.toString().split("[|]");
	String endtime1= splits[3];
	String calltime1= splits[2];
	int stdflag= Integer.parseInt(splits[4]);
	if(stdflag==1)
	{
	Text pno= new Text();
	LongWritable duration_in_minutes= new LongWritable();
	pno.set(splits[0]);
	SimpleDateFormat endtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	SimpleDateFormat starttime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Date endtime2= null;
	Date calltime2=null;
	try {
		endtime2=endtime.parse(endtime1);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try {
		calltime2=starttime.parse(calltime1);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	long dminutes = endtime2.getTime() - calltime2.getTime();
	duration_in_minutes.set(dminutes / (1000 * 60));
	context.write(pno, duration_in_minutes);
	}
	}
}
