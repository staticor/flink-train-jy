package com.imooc.flink.course04;

import course05.DBUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DataSetTransformationApp01 {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

//        mapFunction(env);

//        mapPartitionFunctionDemo(env);

//        firstFunctionDemo(env);

        flatMapFunctionDemo(env);
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i <= 10; i++){
            list.add(i);
        }
        env.fromCollection(list)
                .map((MapFunction<Integer, Integer>) input -> input + 1)
                .filter((FilterFunction<Integer>) input -> input > 5)
                .print();
    }


    public static void mapPartitionFunctionDemo(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        for(int i = 1; i <= 100 ; i ++){
            list.add("student" + i);
        }


        DataSource<String> data = env.fromCollection(list).setParallelism(6);
        data.mapPartition(new MapPartitionFunction<String, Object>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Object> collector) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println("connect = [" + connection + "]");
                DBUtils.returnConnection(connection);
            }
        }).print();

    }


    public static void firstFunctionDemo(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<>();

        info.add(new Tuple2(1, "hadoop"));
        info.add(new Tuple2(1, "spark"));
        info.add(new Tuple2(1, "flink"));
        info.add(new Tuple2(2, "java"));
        info.add(new Tuple2(2, "python"));
        info.add(new Tuple2(3, "linux"));
        info.add(new Tuple2(4, "vue"));
        info.add(new Tuple2(4, "javascript"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        data.groupBy(0).sortGroup(1, Order.ASCENDING)
                .first(2).print();

    }


    public static void flatMapFunctionDemo(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("hadoop,123,spark");
        info.add("hadoop,123,spark");
        info.add("hadoop,123,spark");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>(){

            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for(String split: splits){
                    collector.collect(split);
            }
        }
        }).map(new MapFunction<String, Tuple2<String, Integer>>(){

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        })
        .groupBy(0).sum(1).print();
    }

}
