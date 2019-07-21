package com.damon.flink;

import com.damon.flink.model.Student;
import com.damon.flink.sink.StudentSink;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Hello world!
 *
 */
//@SpringBootApplication
public class App
{
    private static Logger log = Logger.getLogger(App.class.getClass());

    private static Gson gson = new Gson();

	@SuppressWarnings({ "serial", "deprecation" })
	public static void main( String[] args ) throws Exception {

        String topic = "skynet.topic";
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","94.191.65.63:9092");
        properties.setProperty("zookeeper.connect","94.191.65.63:2181");
        properties.setProperty("group.id","test-consumer-group");
        FlinkKafkaConsumer09<String> consumer09 = new FlinkKafkaConsumer09<String>(topic,new SimpleStringSchema(),properties);

        DataStream<String> kafkaStream = env.addSource(consumer09);

        DataStream<Student> studentStream = kafkaStream.map(stuent->gson.fromJson(stuent,Student.class)).keyBy("gender");

        studentStream.addSink(new StudentSink());

        env.execute("Flink Streaming Java API Skeleton");
        log.debug("Flink Started ...");

    }


//s
}
