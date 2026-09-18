package com.notificationservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserPreference {

    private String userId;
    private String preferredChannel; // EMAIL or ZALO
}
