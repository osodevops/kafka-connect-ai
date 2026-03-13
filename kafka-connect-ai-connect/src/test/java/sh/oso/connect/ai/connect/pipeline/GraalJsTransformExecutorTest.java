package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GraalJsTransformExecutorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static GraalJsTransformExecutor executor;

    @BeforeAll
    static void setUp() {
        executor = new GraalJsTransformExecutor(5000, 10);
    }

    @AfterAll
    static void tearDown() {
        executor.close();
    }

    @Test
    void executesSimpleFieldMapping() throws Exception {
        String jsBody = """
                var result = {};
                result.user_name = input.userName;
                result.email = input.emailAddress;
                return result;
                """;

        String inputJson = """
                {"userName": "Alice", "emailAddress": "alice@example.com"}
                """;

        String output = executor.execute(jsBody, inputJson);
        JsonNode result = MAPPER.readTree(output);

        assertEquals("Alice", result.get("user_name").asText());
        assertEquals("alice@example.com", result.get("email").asText());
    }

    @Test
    void executesWithConditionalLogic() throws Exception {
        String jsBody = """
                var result = {};
                result.name = input.firstName + ' ' + input.lastName;
                result.status = input.active ? 'active' : 'inactive';
                result.age = parseInt(input.age) || 0;
                return result;
                """;

        String inputJson = """
                {"firstName": "Alice", "lastName": "Smith", "active": true, "age": "30"}
                """;

        String output = executor.execute(jsBody, inputJson);
        JsonNode result = MAPPER.readTree(output);

        assertEquals("Alice Smith", result.get("name").asText());
        assertEquals("active", result.get("status").asText());
        assertEquals(30, result.get("age").asInt());
    }

    @Test
    void handlesNullValues() throws Exception {
        String jsBody = """
                var result = {};
                result.name = input.name || 'unknown';
                result.email = input.email || null;
                return result;
                """;

        String inputJson = """
                {"name": null}
                """;

        String output = executor.execute(jsBody, inputJson);
        JsonNode result = MAPPER.readTree(output);

        assertEquals("unknown", result.get("name").asText());
    }

    @Test
    void handlesNestedObjectAccess() throws Exception {
        String jsBody = """
                var result = {};
                result.city = input.address && input.address.city ? input.address.city : '';
                result.name = input.user ? input.user.name : '';
                return result;
                """;

        String inputJson = """
                {"user": {"name": "Alice"}, "address": {"city": "London", "zip": "SW1"}}
                """;

        String output = executor.execute(jsBody, inputJson);
        JsonNode result = MAPPER.readTree(output);

        assertEquals("London", result.get("city").asText());
        assertEquals("Alice", result.get("name").asText());
    }

    @Test
    void handlesArrayTransformation() throws Exception {
        String jsBody = """
                var result = {};
                result.items = input.products.map(function(p) {
                    return { id: p.productId, label: p.productName.toUpperCase() };
                });
                result.count = input.products.length;
                return result;
                """;

        String inputJson = """
                {"products": [{"productId": 1, "productName": "Widget"}, {"productId": 2, "productName": "Gadget"}]}
                """;

        String output = executor.execute(jsBody, inputJson);
        JsonNode result = MAPPER.readTree(output);

        assertEquals(2, result.get("count").asInt());
        assertEquals("WIDGET", result.get("items").get(0).get("label").asText());
    }

    @Test
    void validateReturnsTrueForValidCode() {
        String jsBody = "return { name: input.userName || '' };";
        String inputJson = "{\"userName\": \"Alice\"}";

        assertTrue(executor.validate(jsBody, inputJson));
    }

    @Test
    void validateReturnsFalseForInvalidCode() {
        String jsBody = "this is not valid javascript at all!!!";
        String inputJson = "{\"name\": \"Alice\"}";

        assertFalse(executor.validate(jsBody, inputJson));
    }

    @Test
    void validateReturnsFalseForNonJsonOutput() {
        String jsBody = "return 'not json';";
        String inputJson = "{\"name\": \"Alice\"}";

        // "not json" is valid JSON (a string), but let's test something truly non-JSON
        String badJsBody = "return undefined;";
        assertFalse(executor.validate(badJsBody, inputJson));
    }

    @Test
    void throwsTransformExecutionExceptionOnError() {
        String jsBody = "throw new Error('intentional error');";
        String inputJson = "{}";

        assertThrows(GraalJsTransformExecutor.TransformExecutionException.class,
                () -> executor.execute(jsBody, inputJson));
    }

    @Test
    void escapeForJsHandlesSpecialChars() {
        assertEquals("hello\\'world", GraalJsTransformExecutor.escapeForJs("hello'world"));
        assertEquals("line1\\nline2", GraalJsTransformExecutor.escapeForJs("line1\nline2"));
        assertEquals("back\\\\slash", GraalJsTransformExecutor.escapeForJs("back\\slash"));
    }

    @Test
    void executeWithJsonNode() throws Exception {
        String jsBody = "return { upper: input.name.toUpperCase() };";
        JsonNode inputNode = MAPPER.readTree("{\"name\": \"alice\"}");

        JsonNode result = executor.execute(jsBody, inputNode);

        assertEquals("ALICE", result.get("upper").asText());
    }
}
