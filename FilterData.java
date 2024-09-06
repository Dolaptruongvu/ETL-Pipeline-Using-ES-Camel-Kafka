import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.ProducerTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@BindToRegistry("FilterData")
public class FilterData implements Processor {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        JsonNode rootNode = objectMapper.readTree(body);
        JsonNode hitsArray = rootNode.path("hits").path("hits");

        if (hitsArray.isArray() && hitsArray.size() > 0) {
            exchange.setProperty("isEmpty", false); // Nếu có dữ liệu, isEmpty = false
            ProducerTemplate producerTemplate = exchange.getContext().createProducerTemplate();

            for (JsonNode hit : hitsArray) {
                JsonNode sourceNode = hit.path("_source");
                if (sourceNode != null) {
                    int balance = sourceNode.path("balance").asInt(-1);
                    
                    String accountData = sourceNode.toString();
                    if (balance > 21000) {
                        System.out.println("Sending to high-balance-accounts: " + accountData);
                        producerTemplate.sendBody("kafka:high-balance-accounts?brokers=172.27.16.1:9092", accountData);
                    } else {
                        System.out.println("Sending to low-balance-accounts: " + accountData);
                        producerTemplate.sendBody("kafka:low-balance-accounts?brokers=172.27.16.1:9092", accountData);
                    }
                }
            }
        } else {
            exchange.setProperty("isEmpty", true); // Nếu không có dữ liệu, isEmpty = true
            System.out.println("No hits found.");
        }
    }
}
