package de.tuberlin.dima.bdapro.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableJms
public class JmsConfig {


//    @Bean
//    public ActiveMQConnectionFactory connectionFactory(){
//        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
//        connectionFactory.setBrokerURL(BROKER_URL);
//        connectionFactory.setPassword(BROKER_USERNAME);
//        connectionFactory.setUserName(BROKER_PASSWORD);
//        return connectionFactory;
//    }
//
//    ConnectionFactory connectionFactory() {
//        ConnectionFactory connectionFactory = new ConnectionFactory();
//        connectionFactory.setUsername(properties.getMessaging().getUser());
//        connectionFactory.setPassword(properties.getMessaging().getPassword());
//        connectionFactory.setPort(Integer.parseInt(properties.getMessaging().getPort()));
//        return connectionFactory;
//    }

//    @Autowired
//    JmsTemplate jmsTemplate;


//    @Bean
//    public JmsTemplate jmsTemplate(){
//        JmsTemplate template = new JmsTemplate();
//        template.setConnectionFactory(connectionFactory());
//        return template;
//    }
//
//    @Bean
//    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
//        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
//        factory.setConnectionFactory(connectionFactory());
//        factory.setConcurrency("1-1");
//        return factory;
//    }
    
    
    /* see:
     * https://springbootdev.com/2017/09/15/spring-boot-and-rabbitmq-direct-exchange-example-messaging-custom-java-objects-and-consumes-with-a-listener/
     */
    
    
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
