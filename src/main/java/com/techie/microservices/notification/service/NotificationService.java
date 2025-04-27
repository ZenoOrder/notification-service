package com.techie.microservices.notification.service;

import com.techie.microservices.order.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j // for logging
public class NotificationService {

    private final JavaMailSender javaMailSender;

    @KafkaListener(topics = "order-placed")
    public void listen(OrderPlacedEvent orderPlacedEvent) {
        log.info("Got message from order-placed topic {}", orderPlacedEvent);
        MimeMessagePreparator mimeMessagePreparator = mimeMessage -> {
            MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage);
            messageHelper.setFrom("springshop@email.com");
            messageHelper.setTo(orderPlacedEvent.getEmail().toString());
            log.info(orderPlacedEvent.getEmail(), orderPlacedEvent.getOrderNumber());
            messageHelper.setSubject(String.format("Your order with orderNumber %s is placed successfully", orderPlacedEvent.getOrderNumber()));
            messageHelper.setText(String.format("""
                    Hi
                    
                    Your order with number %s is now placed successfully.
                    
                    
                    Best Regards
                    Spring Shop
                    """, orderPlacedEvent.getOrderNumber())
                    );
        };

        try{
            javaMailSender.send(mimeMessagePreparator);
            log.info("Order Notification email sent!!");
        } catch(MailException e) {
            log.error("Exception occurred while sending email", e);
            throw new RuntimeException("Exception occurred when sending email to springshop@email.com" ,e);
        }

    }


}
