import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

@Configuration
@BindToRegistry("RegistryScrollId")
public class RegistryScrollId implements Processor {

    public void process(Exchange exchange) throws Exception {
    // Lấy giá trị scroll_id từ Exchange
    String scrollId = exchange.getProperty("scroll_id", String.class);
    
    // Kiểm tra và log giá trị của scrollId trước khi tiếp tục xử lý
    if (scrollId != null && !scrollId.isEmpty()) {
        // Log giá trị scrollId
        System.out.println("Scroll ID: " + scrollId);

        // Kiểm tra nếu scrollId có thể là JSON
        if (scrollId.trim().startsWith("{")) {
            // Xử lý tiếp với Jackson nếu là JSON
            exchange.getContext().getRegistry().bind("scrollIdRegistry", scrollId);
            exchange.getIn().setBody("Scroll ID has been stored in the registry: " + scrollId);
        } else {
            // Nếu scrollId không phải JSON hợp lệ, log cảnh báo và dừng
            exchange.getIn().setBody("Scroll ID is not a valid JSON: " + scrollId);
        }
    } else {
        // Nếu không có scrollId, log lỗi
        exchange.getIn().setBody("Scroll ID is null or empty.");
    }
}

}
