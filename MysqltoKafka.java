import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@BindToRegistry("MysqltoKafka")
public class MysqltoKafka implements Processor {

    private ObjectMapper objectMapper = new ObjectMapper();

    // Method to set up the connection to MySQL
    private Connection getConnection() throws SQLException, ClassNotFoundException {
        // Load MySQL driver
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Configure MySQL connection
        String url = "mysqlurl";
        String user = "user";
        String password = "pass";

        // Return the connection
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        ProducerTemplate producerTemplate = exchange.getContext().createProducerTemplate();

        try {
            // Retrieve offset from exchange property, default to 0 if not present
            Integer offset = exchange.getProperty("offset", 0, Integer.class);

            // Set up the connection to MySQL
            connection = getConnection();

            // Create SQL query with LIMIT and OFFSET
            String sql = "SELECT * FROM accounts LIMIT 100 OFFSET ?";

            // Prepare SQL statement
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, offset);

            // Execute query
            resultSet = preparedStatement.executeQuery();
            Integer count = 0;

            // Print data to console and send to Kafka
            while (resultSet.next()) {
                int accountNumber = resultSet.getInt("account_number");
                int balance = resultSet.getInt("balance");
                String firstname = resultSet.getString("firstname");
                String lastname = resultSet.getString("lastname");
                int age = resultSet.getInt("age");
                String gender = resultSet.getString("gender");
                String address = resultSet.getString("address");
                String employer = resultSet.getString("employer");
                String email = resultSet.getString("email");
                String city = resultSet.getString("city");
                String state = resultSet.getString("state");
                String companyCode = resultSet.getString("companyCode");

                // Create JSON object
                ObjectNode accountData = objectMapper.createObjectNode();
                accountData.put("accountNumber", accountNumber);
                accountData.put("balance", balance);
                accountData.put("firstname", firstname);
                accountData.put("lastname", lastname);
                accountData.put("age", age);
                accountData.put("gender", gender);
                accountData.put("address", address);
                accountData.put("employer", employer);
                accountData.put("email", email);
                accountData.put("city", city);
                accountData.put("state", state);
                accountData.put("companyCode", companyCode);

                // Convert JSON to string
                String accountDataJson = accountData.toString();

                // Send data to Kafka based on companyCode
                if ("A01".equals(companyCode)) {
                    producerTemplate.sendBody("kafka:Company-A01?brokers=localhost:9092", accountDataJson);
                } else if ("A02".equals(companyCode)) {
                    producerTemplate.sendBody("kafka:Company-A02?brokers=localhost:9092", accountDataJson);
                }

                count++;
            }
            // Check if results are less than 100 records, end process
            if (count < 100) {
                exchange.setProperty("isEmpty", true);
            } else {
                exchange.setProperty("isEmpty", false);
            }
            Integer total_processed = exchange.getProperty("total_processed", 0, Integer.class);
            total_processed += count;
            exchange.setProperty("total_processed", total_processed);
            // Increment offset and save to exchange property
            offset += 100;
            exchange.setProperty("offset", offset);

        } catch (SQLException | ClassNotFoundException e) {
            // Catch exceptions related to connection or SQL query
            throw new RuntimeException("Error processing MySQL data", e);
        } finally {
            // Ensure to close connections and resources
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
