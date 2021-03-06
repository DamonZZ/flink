package com.damon.flink.sink;

import com.damon.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StudentSink extends RichSinkFunction<Student> {

    private static Logger log = LoggerFactory.getLogger(StudentSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception
    {
        super.close();
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        log.info("Student : "+value.getName()+", Score : "+value.getScore());
    }
}
