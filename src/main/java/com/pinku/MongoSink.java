package com.pinku;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.pinku.pojos.Employee;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class MongoSink implements SinkFunction<Employee> {
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "flinkcompanydb";
    private static final String COLLECTION_NAME = "company";

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    @Override
    public void invoke(Employee employee, Context context) {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(MONGO_URI);
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            collection = database.getCollection(COLLECTION_NAME);
        }

        Document doc = new Document("id", employee.getId())
                .append("name", employee.getName())
                .append("age", employee.getAge())
                .append("salary", employee.getSalary())
                .append("timestamp", System.currentTimeMillis());
        collection.insertOne(doc);
    }
}