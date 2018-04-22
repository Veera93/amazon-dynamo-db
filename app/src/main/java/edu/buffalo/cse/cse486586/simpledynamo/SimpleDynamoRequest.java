package edu.buffalo.cse.cse486586.simpledynamo;


/**
 * Created by veera on 4/22/18.
 */

class SimpleDynamoRequest {

    /*********
    type = insert               : id; key; value


    **********/

    private String originator;
    private String type;
    private String arguments;

    public SimpleDynamoRequest() {

    }

    public SimpleDynamoRequest(String sender, String type, String arguments) {
        this.originator = sender;
        this.type = type;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        return originator + SimpleDynamoConfiguration.DELIMITER + type + SimpleDynamoConfiguration.DELIMITER + arguments;
    }

    static final class Type {
        // ToDo: keep the types as static final String
        static final String INSERT = "insert";
    }
}


