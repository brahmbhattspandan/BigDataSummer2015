import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.io.IOException;


public class MyJob extends Configured implements Tool {
    public static class MapClass
            extends TableMapper<Text, IntWritable> {

        public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
                throws IOException, InterruptedException {

            try {
                // get rowKey and convert it to string
                String inKey = new String(rowKey.get());
                // set new key having only date
                String oKey = inKey.split("#")[0];
                // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
                byte[] bSales = columns.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("sales"));
                String sSales = new String(bSales);
                Integer sales = new Integer(sSales);
                // emit date and sales values
                context.write(new Text(oKey), new IntWritable(sales));
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            try {
                int sum = 0;
                // loop through different sales vales and add it to sum
                for (IntWritable sales : values) {
                    Integer intSales = new Integer(sales.toString());
                    sum += intSales;
                }

                // create hbase put with rowkey as date
                Put insHBase = new Put(key.getBytes());
                // insert sum value to hbase
                insHBase.add(Bytes.toBytes("cf1"), Bytes.toBytes("sum"), Bytes.toBytes(sum));
                // write data to Hbase table
                context.write(null, insHBase);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // define scan and define column families to scan
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes("cf1"));

        Job job = new Job(conf);

        job.setJarByClass(MyJob.class);
        // define input hbase table
        TableMapReduceUtil.initTableMapperJob(
                "test1",
                scan,
                MapClass.class,
                Text.class,
                IntWritable.class,
                job);
        // define output table
        TableMapReduceUtil.initTableReducerJob(
                "test2",
                Reduce.class,
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}