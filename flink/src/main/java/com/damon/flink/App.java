package com.damon.flink;

import com.damon.flink.service.KafkaService;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App
{

	@SuppressWarnings({ "serial", "deprecation" })
	public static void main( String[] args )
    {
        SpringApplication.run(App.class,args);

//        String key = "Damon";
//        Date date = new Date();
//        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
//        String value = dateFormat.format(date);
//        new KafkaService().SendMessage(key,value);

//		System.out.println(",,,,,,,");
//
//
//        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
//
//		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
//          .keyBy(new KeySelector<WikipediaEditEvent, String>() {
//            public String getKey(WikipediaEditEvent event) {
//              return event.getUser();
//            }
//          });
//
//		DataStream<Tuple2<String, Long>> result = keyedEdits
//          .timeWindow(Time.seconds(5))
//          .fold(new Tuple2<String, Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
//              acc.f0 = event.getUser();
//              acc.f1 += event.getByteDiff();
//              return acc;
//            }
//          });
//
//        result.print();
//
//        see.execute();
    }
}
