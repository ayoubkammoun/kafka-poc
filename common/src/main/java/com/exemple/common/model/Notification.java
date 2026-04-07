package com.exemple.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    private String notificationId;
    private String recipient;
    private String message;
    private NotificationType type;

    public enum NotificationType {
        EMAIL, SMS, PUSH
    }
}
