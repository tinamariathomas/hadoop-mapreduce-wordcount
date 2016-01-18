import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountReducerTest {
    ReduceDriver reduceDriver;
    WordCountReducer reducer;

    @Before
    public void setUp() {
        reducer = new WordCountReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    @Test
    public void testMap() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(4));
        values.add(new IntWritable(5));
        IntWritable expectedOutput = new IntWritable(9);
        reduceDriver.withInput(new Text("hello"), values);
        reduceDriver.withOutput(new Text("hello"), expectedOutput);
        reduceDriver.runTest();

    }

}