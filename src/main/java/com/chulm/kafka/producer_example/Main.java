package com.chulm.kafka.producer_example;

public class Main {
    public static void main(String[] args){

        long start  = System.currentTimeMillis();
        try {
            MyKafkaProducer.runProducerSync(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.err.println("producer end sync time = "  + (System.currentTimeMillis() - start));

        long start2  = System.currentTimeMillis();
        try {
            MyKafkaProducer.runProducerAsync(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.err.println("producer end async time = "  + (System.currentTimeMillis() - start2));
    }
}
