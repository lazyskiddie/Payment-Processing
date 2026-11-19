package com.event.payment;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentProcessor {

    private static final BigDecimal DECLINE_ABOVE = new BigDecimal("1000000");

    private final PaymentRepository paymentRepository;

    @Value("${payment.bank.latency-ms:50}")
    private long bankLatencyMs;

    @KafkaListener(topics = PaymentService.TOPIC)
    public void consumePaymentEvent(PaymentEvent event) {
        log.info("Received PaymentEvent for payment {}", event.paymentId());

        // FIX: CLAIM FIRST, call the bank second. The conditional UPDATE returns 1 for exactly one
        // consumer, so a duplicate event (reconciler republish, redelivery, two consumers) can never
        // trigger a second bank call.
        int claimed = paymentRepository.markProcessing(event.paymentId(), LocalDateTime.now());
        if (claimed == 0) {
            if (!paymentRepository.existsById(event.paymentId())) {
                // goes to the error handler: retried a few times, then to payment-events.DLT
                throw new IllegalStateException("Payment not found: " + event.paymentId());
            }
            log.info("Payment {} already claimed or finished; skipping", event.paymentId());
            return;
        }

        boolean approved;
        try {
            approved = callBank(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // stays PROCESSING; the reconciler resets it to PENDING after the timeout
            throw new IllegalStateException("Interrupted while calling bank", e);
        }

        if (approved) {
            paymentRepository.markCompleted(event.paymentId(), LocalDateTime.now());
            log.info("Payment {} COMPLETED", event.paymentId());
        } else {
            paymentRepository.markFailed(event.paymentId(), "Declined: amount exceeds limit", LocalDateTime.now());
            log.warn("Payment {} declined", event.paymentId());
        }
    }

    // Simulated bank. With a real bank, send event.idempotencyKey() downstream so the bank can de-duplicate too.
    private boolean callBank(PaymentEvent event) throws InterruptedException {
        Thread.sleep(bankLatencyMs);
        return event.amount().compareTo(DECLINE_ABOVE) <= 0;
    }
}