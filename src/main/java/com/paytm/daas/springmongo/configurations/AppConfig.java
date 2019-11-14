package com.paytm.daas.springmongo.configurations;

import com.mongodb.MongoClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

@Configuration
public class AppConfig {

    private String host;
    private Integer port;
    private String databaseName;

    @Autowired
    public AppConfig(@Value("${mongodb.host}") String host,@Value("${mongodb.port}") Integer port,
                     @Value("${mongodb.database}") String databaseName) {
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
    }

    public @Bean
    MongoDbFactory mongoDbFactory() throws Exception {
        return new SimpleMongoDbFactory(new MongoClient(host, port), databaseName);
    }

    public @Bean
    MongoOperations mongoTemplate() throws Exception {
        return new MongoTemplate(mongoDbFactory());
    }
}
