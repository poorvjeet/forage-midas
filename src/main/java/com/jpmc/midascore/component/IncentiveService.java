package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private final RestTemplate restTemplate;

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive query(Transaction transaction) {
        String url = "http://localhost:33433/incentive";
        return restTemplate.postForObject(url, transaction, Incentive.class);
    }
}
