package com.diwakaru.diwakar.runner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class WebhookRunner implements CommandLineRunner {

    private final RestTemplate restTemplate;

    public WebhookRunner(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public void run(String... args) throws Exception {

        // STEP 1: CALL generateWebhook API
        String generateUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("name", "Diwakar Upadhyay");
        requestBody.put("regNo", "22BPS1212");
        requestBody.put("email", "diwakar.upadhyay2022@vitstudent.ac.in");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

        // OPTIONAL: see raw response for debugging
        String raw = restTemplate.postForObject(generateUrl, request, String.class);
        System.out.println("RAW RESPONSE = " + raw);

        // Now parse into our object
        GenerateWebhookResponse response =
                restTemplate.postForObject(generateUrl, request, GenerateWebhookResponse.class);

        if (response == null || response.getWebhookUrl() == null || response.getAccessToken() == null) {
            System.out.println("ERROR: Failed to get webhook or accessToken");
            return;
        }

        String webhookUrl = response.getWebhookUrl();
        String accessToken = response.getAccessToken();

        System.out.println("Parsed webhook URL = " + webhookUrl);
        System.out.println("Parsed access token = " + accessToken);

        // STEP 2: FINAL SQL QUERY (ANSWER FOR QUESTION 2)
        String finalSQL =
                "SELECT d.department_name, "
                        + "AVG(TIMESTAMPDIFF(YEAR, e.dob, CURDATE())) AS average_age, "
                        + "GROUP_CONCAT(emp_name ORDER BY emp_name SEPARATOR ', ') AS employee_list "
                        + "FROM ( "
                        + "SELECT DISTINCT e.emp_id, e.first_name, e.last_name, "
                        + "CONCAT(e.first_name, ' ', e.last_name) AS emp_name, e.department "
                        + "FROM employee e JOIN payments p ON e.emp_id = p.emp_id "
                        + "WHERE p.amount > 70000 LIMIT 10 "
                        + ") AS high_salary "
                        + "JOIN department d ON high_salary.department = d.department_id "
                        + "JOIN employee e ON high_salary.emp_id = e.emp_id "
                        + "GROUP BY d.department_id, d.department_name "
                        + "ORDER BY d.department_id DESC;";

        // STEP 3: SUBMIT SQL QUERY TO WEBHOOK
        HttpHeaders submitHeaders = new HttpHeaders();
        submitHeaders.setBearerAuth(accessToken);
        submitHeaders.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> submitBody = new HashMap<>();
        submitBody.put("finalQuery", finalSQL);

        HttpEntity<Map<String, String>> submitRequest =
                new HttpEntity<>(submitBody, submitHeaders);

        String submitResponse =
                restTemplate.postForObject(webhookUrl, submitRequest, String.class);

        System.out.println("Submission Response = " + submitResponse);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class GenerateWebhookResponse {

        // MAP THE FIELD NAMED "webhook" IN JSON
        @JsonProperty("webhook")
        private String webhookUrl;

        @JsonProperty("accessToken")
        private String accessToken;

        public String getWebhookUrl() {
            return webhookUrl;
        }

        public String getAccessToken() {
            return accessToken;
        }
    }
}
