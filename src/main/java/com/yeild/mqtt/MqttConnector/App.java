package com.yeild.mqtt.MqttConnector;

import com.yeild.mqtt.MqttServerTask;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        new MqttServerTask("").start();
    }
}
