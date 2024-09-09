import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@BindToRegistry("ProcessQueryAndSendtoKafka")
public class ProcessQueryAndSendtoKafka implements Processor {

    private ObjectMapper objectMapper = new ObjectMapper();

    // Method to set up the connection to MySQL
    private Connection getConnection() throws SQLException, ClassNotFoundException {
        // Load MySQL driver
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Configure MySQL connection
        String url = "jdbc:mysql://host:port/defaultdb";
        String user = "user";
        String password = "pass";

        // Return the connection
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        // Get JSON body from Elasticsearch (hits)
        String body = exchange.getIn().getBody(String.class);
        JsonNode rootNode = objectMapper.readTree(body);
        JsonNode hitsArray = rootNode.path("hits").path("hits");

        if (hitsArray.isArray() && hitsArray.size() > 0) {
            // Set property to indicate data is present
            exchange.setProperty("isEmpty", false);

            // Create ProducerTemplate for sending data to Kafka
            ProducerTemplate producerTemplate = exchange.getContext().createProducerTemplate();
            Integer count = 0;
            // Iterate over each hit in the hits array
            for (JsonNode hit : hitsArray) {
                JsonNode sourceNode = hit.path("_source");
                String indexName = hit.path("_index").asText();  // Get the index of the current hit

                if (sourceNode != null) {
                    // Check if index is "accounts"
                    String companyCode;
                    if ("accounts".equals(indexName)) {
                        companyCode = "A01";  
                    } else {
                        companyCode = "A02"; 
                    }

                    // Add companyCode to the source node
                    ((ObjectNode) sourceNode).put("companyCode", companyCode);

                    // Query MySQL to get the Kafka topic based on companyCode
                    Connection connection = null;
                    PreparedStatement preparedStatement = null;
                    ResultSet resultSet = null;

                    try {
                        // Set up MySQL connection
                        connection = getConnection();

                        // Query to fetch Kafka topic based on companyCode
                        String sql = "SELECT topic FROM company WHERE companyCode = ?";
                        preparedStatement = connection.prepareStatement(sql);
                        preparedStatement.setString(1, companyCode);

                        // Execute the query
                        resultSet = preparedStatement.executeQuery();

                        // If we get results, process the data
                        if (resultSet.next()) {
                            String kafkaTopic = resultSet.getString("topic");

                            // Convert JSON to string
                            String accountDataJson = sourceNode.toString();

                            // Send data to the Kafka topic based on companyCode
                            System.out.println("Sending to Kafka topic: " + kafkaTopic);
                            producerTemplate.sendBody("kafka:" + kafkaTopic + "?brokers=172.27.16.1:9092", accountDataJson);
                            count++;
                        }

                    } catch (SQLException | ClassNotFoundException e) {
                        throw new RuntimeException("Error querying MySQL", e);
                    } finally {
                        // Ensure the MySQL connection and statement are closed
                        
                        if (resultSet != null) {
                            resultSet.close();
                        }
                        if (preparedStatement != null) {
                            preparedStatement.close();
                        }
                        if (connection != null) {
                            connection.close();
                        }
                    }
                }
            }
            Integer updateCount = exchange.getProperty("total_processed",0,Integer.class);
            updateCount += count;
            exchange.setProperty("total_processed",updateCount);
        } else {
            // If no hits found in Elasticsearch data
            exchange.setProperty("isEmpty", true);
            System.out.println("No hits found.");
        }
    }
}
