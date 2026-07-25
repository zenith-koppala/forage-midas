package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveClient incentiveClient;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository, IncentiveClient incentiveClient) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveClient = incentiveClient;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    @Transactional
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        boolean valid = sender != null && recipient != null
                && sender.getBalance() >= transaction.getAmount();

        if (valid) {
            Incentive incentive = incentiveClient.fetchIncentive(transaction);
            float incentiveAmount = incentive != null ? incentive.getAmount() : 0f;

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
            userRepository.save(sender);
            userRepository.save(recipient);

            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
            record.setIncentive(incentiveAmount);
            transactionRepository.save(record);
        }
    }
}