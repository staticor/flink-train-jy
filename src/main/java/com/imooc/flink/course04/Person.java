package com.imooc.flink.course04;

public class Person {
    public String name;
    public int age;
    public String job;

    public Person(){
        this("",  0, "");
    }

    public Person(String n, int a, String j){
        name = n ;
        age = a;
        job = j;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", job='" + job + '\'' +
                '}';
    }
}
