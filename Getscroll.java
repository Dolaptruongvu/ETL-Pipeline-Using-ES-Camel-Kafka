import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
@Configuration
@BindToRegistry("Getscroll")
public class Getscroll implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String body = exchange.getIn().getBody(String.class);

        JsonNode rootNode = mapper.readTree(body);
        if (rootNode.has("_scroll_id")) {
            String scrollId = rootNode.path("_scroll_id").asText();
            // System.out.println("Scroll ID found: " + scrollId);  // Debug
            // Set scroll_id to exchange property
            exchange.setProperty("scroll_id", scrollId);
        } else {
            System.out.println("No scroll ID found in the response.");
        }
    }
}
