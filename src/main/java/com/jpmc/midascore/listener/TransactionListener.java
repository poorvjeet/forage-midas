package com.jpmc.midascore.listener;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    public TransactionListener(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            Incentive incentive = incentiveService.query(transaction);
            if (incentive != null && incentive.getAmount() > 0) {
                recipient.setBalance(recipient.getBalance() + incentive.getAmount());
            }

            databaseConduit.save(sender);
            databaseConduit.save(recipient);
        }
    }
}
