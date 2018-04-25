package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by veera on 4/22/18.
 */

public class SimpleDynamoResponse {
    /*********



     **********/

    private String sender;
    private String type;
    private String arguments;

    public SimpleDynamoResponse(String sender, String type, String arguments) {
        this.sender = sender;
        this.type = type;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        return sender + SimpleDynamoConfiguration.DELIMITER + type + SimpleDynamoConfiguration.DELIMITER + arguments;
    }

    static final class Type {
        static final String INSERT = "insertResponse";
        static final String REPLICATE = "replicateResponse";
    }
}
