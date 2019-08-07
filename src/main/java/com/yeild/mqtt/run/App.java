package com.yeild.mqtt.run;

import com.yeild.mqtt.MqttConnector;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        new Thread(new MqttConnector("")).start();
    }
}
