package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class PiiMaskerTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void mask_fieldNameMatch() throws Exception {
        PiiMasker masker = new PiiMasker(Set.of("ssn", "email"), List.of(), "[MASKED]", true);

        String json = "{\"name\":\"Alice\",\"ssn\":\"123-45-6789\",\"email\":\"alice@example.com\"}";
        PiiMasker.MaskResult result = masker.mask(json);

        JsonNode masked = mapper.readTree(result.maskedJson());
        assertEquals("Alice", masked.get("name").asText());
        assertTrue(masked.get("ssn").asText().startsWith("[MASKED_"));
        assertTrue(masked.get("email").asText().startsWith("[MASKED_"));
        assertEquals(2, result.maskMap().size());
    }

    @Test
    void mask_caseInsensitiveFieldMatch() throws Exception {
        PiiMasker masker = new PiiMasker(Set.of("email"), List.of(), "[MASKED]", true);

        String json = "{\"Email\":\"alice@example.com\",\"EMAIL\":\"bob@example.com\"}";
        PiiMasker.MaskResult result = masker.mask(json);

        JsonNode masked = mapper.readTree(result.maskedJson());
        assertTrue(masked.get("Email").asText().startsWith("[MASKED_"));
        assertTrue(masked.get("EMAIL").asText().startsWith("[MASKED_"));
    }

    @Test
    void mask_regexPatternMatch() throws Exception {
        Pattern emailPattern = Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}");
        PiiMasker masker = new PiiMasker(Set.of(), List.of(emailPattern), "[MASKED]", true);

        String json = "{\"contact\":\"alice@example.com\",\"name\":\"Alice\"}";
        PiiMasker.MaskResult result = masker.mask(json);

        JsonNode masked = mapper.readTree(result.maskedJson());
        assertTrue(masked.get("contact").asText().startsWith("[MASKED_"));
        assertEquals("Alice", masked.get("name").asText());
    }

    @Test
    void mask_nestedFields() throws Exception {
        PiiMasker masker = new PiiMasker(Set.of("ssn"), List.of(), "[MASKED]", true);

        String json = "{\"user\":{\"name\":\"Alice\",\"ssn\":\"123-45-6789\"}}";
        PiiMasker.MaskResult result = masker.mask(json);

        JsonNode masked = mapper.readTree(result.maskedJson());
        assertTrue(masked.get("user").get("ssn").asText().startsWith("[MASKED_"));
        assertEquals("Alice", masked.get("user").get("name").asText());
    }

    @Test
    void mask_arrayInput() throws Exception {
        PiiMasker masker = new PiiMasker(Set.of("ssn"), List.of(), "[MASKED]", true);

        String json = "[{\"ssn\":\"111\"},{\"ssn\":\"222\"}]";
        PiiMasker.MaskResult result = masker.mask(json);

        JsonNode masked = mapper.readTree(result.maskedJson());
        assertTrue(masked.isArray());
        assertEquals(2, masked.size());
        assertTrue(masked.get(0).get("ssn").asText().startsWith("[MASKED_"));
        assertTrue(masked.get(1).get("ssn").asText().startsWith("[MASKED_"));
        assertEquals(2, result.maskMap().size());
    }

    @Test
    void unmask_replacesPlaceholders() {
        PiiMasker masker = new PiiMasker(Set.of("ssn"), List.of(), "[MASKED]", true);

        Map<String, String> maskMap = Map.of("[MASKED_0]", "123-45-6789", "[MASKED_1]", "alice@example.com");
        String llmOutput = "SSN is [MASKED_0] and email is [MASKED_1]";

        String unmasked = masker.unmask(llmOutput, maskMap);

        assertEquals("SSN is 123-45-6789 and email is alice@example.com", unmasked);
    }

    @Test
    void unmask_disabledReturnsOriginal() {
        PiiMasker masker = new PiiMasker(Set.of("ssn"), List.of(), "[MASKED]", false);

        Map<String, String> maskMap = Map.of("[MASKED_0]", "123-45-6789");
        String llmOutput = "SSN is [MASKED_0]";

        String result = masker.unmask(llmOutput, maskMap);

        assertEquals("SSN is [MASKED_0]", result);
    }

    @Test
    void isEnabled_falseWhenEmpty() {
        PiiMasker masker = new PiiMasker(Set.of(), List.of(), "[MASKED]", true);
        assertFalse(masker.isEnabled());
    }

    @Test
    void isEnabled_trueWithFields() {
        PiiMasker masker = new PiiMasker(Set.of("ssn"), List.of(), "[MASKED]", true);
        assertTrue(masker.isEnabled());
    }

    @Test
    void isEnabled_trueWithPatterns() {
        PiiMasker masker = new PiiMasker(Set.of(), List.of(Pattern.compile("\\d{3}-\\d{2}-\\d{4}")), "[MASKED]", true);
        assertTrue(masker.isEnabled());
    }
}
