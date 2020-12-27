package com.imooc.flink.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

public class DataSetDataSourceApp01 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        fromLocalFile(env);
        System.out.println("--------------------");
        fromLocalDirectory(env);
        System.out.println("--------------------");
        fromLocalCSV(env);
    }

    // 从本地集合读取数据
    private static void fromCollection(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> alist = new ArrayList<Integer>();
        for (int i = 0; i < 26; i++) {
            alist.add(i);
        }
        env.fromCollection(alist).print();
    }

    private static void fromLocalFile(ExecutionEnvironment env) throws Exception {
        String filename = "file:///Users/staticor/FlinkProject/data01/test.data";
        DataSource<String> dataFromFile = env.readTextFile(filename);
        dataFromFile.print();
    }

    private static void fromLocalDirectory(ExecutionEnvironment env) throws Exception {
        String filename = "file:///Users/staticor/FlinkProject/data02/";
        DataSource<String> dataFromFile = env.readTextFile(filename);
        dataFromFile.print();
    }

    private static void fromLocalCSV(ExecutionEnvironment env) throws Exception {
        String filename = "file:///Users/staticor/FlinkProject/csv/test.data";
        env.readCsvFile(filename).ignoreFirstLine().pojoType(Person.class, "name", "age", "job").print();
    }
}
