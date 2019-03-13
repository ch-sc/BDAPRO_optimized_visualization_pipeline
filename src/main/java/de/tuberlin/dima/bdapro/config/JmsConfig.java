package de.tuberlin.dima.bdapro.config;

import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableJms
public class JmsConfig {
    
    @Value("${spring.rabbitmq.host:localhost}")
    String host;
    @Value("${spring.rabbitmq.port:5672}")
    Integer port;
    @Value("${spring.rabbitmq.username:user}")
    String user;
    @Value("${spring.rabbitmq.password:password}")
    String password;


    /*
     * see:
     * https://springbootdev.com/2017/09/15/spring-boot-and-rabbitmq-direct-exchange-example-messaging-custom-java-objects-and-consumes-with-a-listener/
     */
    
    @Bean
    public RMQConnectionConfig rmqConnectionConfig(){
        return new RMQConnectionConfig.Builder()
                .setHost(host)
                .setVirtualHost("/")
                .setPort(port)
                .setUserName(user)
                .setPassword(password)
                .build();
    }
    
    @Bean
    public MessageConverter jsonMessageConverter()
    {
        return new Jackson2JsonMessageConverter();
    }
    
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory)
    {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }
    
}
