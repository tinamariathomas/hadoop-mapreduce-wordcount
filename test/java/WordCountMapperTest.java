import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WordCountMapperTest {
    MapDriver mapDriver;
    WordCountMapper mapper;
    @Before
    public void setUp() {
        mapper = new WordCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

    }
    @Test
    public void testMap() throws IOException {
        mapDriver.withInput(new LongWritable(1),new Text("hello there mister"));
        mapDriver.withOutput(new Text("hello"),new IntWritable(1));
        mapDriver.withOutput(new Text("there"),new IntWritable(1));
        mapDriver.withOutput(new Text("mister"),new IntWritable(1));
        mapDriver.runTest();
    }
}
