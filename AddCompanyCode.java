import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Configuration
@BindToRegistry("AddCompanyCode")
public class AddCompanyCode implements Processor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Method to establish a MySQL connection with SSL enabled and certificate verification disabled
    private Connection getConnection() throws SQLException, ClassNotFoundException {
        // Ensure the MySQL driver is loaded
        Class.forName("com.mysql.cj.jdbc.Driver");
        
        // Connection configuration with SSL enabled but certificate verification disabled
        String url = "jdbc:mysql://host:port/defaultdb"
                   + "?useSSL=true&verifyServerCertificate=false&requireSSL=true";
        String user = "user";
        String password = "pass";
        
        // Establish a connection to MySQL
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Get the JSON body from the Exchange
            String jsonBody = exchange.getIn().getBody(String.class);
            JsonNode rootNode = objectMapper.readTree(jsonBody);
            JsonNode hitsArrayNode = rootNode.path("hits").path("hits");

            // Check if hitsArrayNode is empty and set the exchange property accordingly
            if (!hitsArrayNode.isArray() || hitsArrayNode.size() == 0) {
                // Set exchange property to indicate the array is empty
                System.out.println("is empty");
                exchange.setProperty("isEmpty", true);
                return;  // Stop processing if the hits array is empty
            } else {
                // Set exchange property to indicate the array is not empty
                System.out.println("not empty");
                System.out.println("hits data"+hitsArrayNode.toString());
                exchange.setProperty("isEmpty", false);
            }

            // Establish a MySQL connection
            connection = getConnection();

            // Prepare the SQL statement for inserting data
            String sql = "INSERT INTO accounts (account_number, balance, firstname, lastname, age, gender, address, employer, email, city, state, companyCode) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = connection.prepareStatement(sql);

            // Loop through each element in 'hits'
            Integer totalProcessed = exchange.getProperty("total_processed", 0, Integer.class);
            int count = 0;
            for (JsonNode hit : hitsArrayNode) {
                ObjectNode sourceNode = (ObjectNode) hit.path("_source");

                // Get the account_number value and check if it's 0
                int accountNumber = sourceNode.path("account_number").asInt();
                if (accountNumber == 0) {
                    // Skip the record if account_number is 0
                    continue;
                }

                // Check if _index is "accounts" and add companyCode accordingly
                String companyCode;
                if (hit.path("_index").asText().equals("accounts")) {
                    companyCode = "A01";
                    System.out.println("Processing A01 record: " + sourceNode.toString());
                    count++;
                } else {
                    companyCode = "A02";
                    // Log when A02 is processed
                    System.out.println("Processing A02 record: " + sourceNode.toString());
                    count++;
                }
                sourceNode.put("companyCode", companyCode);

                // Set the values in the SQL statement
                preparedStatement.setInt(1, accountNumber);
                preparedStatement.setInt(2, sourceNode.path("balance").asInt());
                preparedStatement.setString(3, sourceNode.path("firstname").asText());
                preparedStatement.setString(4, sourceNode.path("lastname").asText());
                preparedStatement.setInt(5, sourceNode.path("age").asInt());
                preparedStatement.setString(6, sourceNode.path("gender").asText());
                preparedStatement.setString(7, sourceNode.path("address").asText());
                preparedStatement.setString(8, sourceNode.path("employer").asText());
                preparedStatement.setString(9, sourceNode.path("email").asText());
                preparedStatement.setString(10, sourceNode.path("city").asText());
                preparedStatement.setString(11, sourceNode.path("state").asText());
                preparedStatement.setString(12, sourceNode.path("companyCode").asText());

                // Execute the SQL statement to insert data into MySQL
                preparedStatement.executeUpdate();
            }
            totalProcessed += count;
            exchange.setProperty("total_processed",totalProcessed);

        } catch (SQLException | ClassNotFoundException e) {
            // Throw an exception if there's a connection or SQL error
            throw new RuntimeException("Connection or SQL error", e);
        } finally {
            // Ensure the PreparedStatement and Connection are closed
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
