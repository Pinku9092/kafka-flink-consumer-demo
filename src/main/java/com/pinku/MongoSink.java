package com.pinku;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class MongoSink implements SinkFunction<String> {
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "flinkdb";
    private static final String COLLECTION_NAME = "messages";

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    @Override
    public void invoke(String value, Context context) {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(MONGO_URI);
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            collection = database.getCollection(COLLECTION_NAME);
        }

        Document doc = new Document("message", value)
                .append("timestamp", System.currentTimeMillis());
        collection.insertOne(doc);
    }
}