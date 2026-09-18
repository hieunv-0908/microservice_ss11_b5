package com.notificationservice.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient() {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .responseTimeout(Duration.ofMillis(3000)) // readTimeout 3 giây
                .doOnConnected(conn -> 
                        conn.addHandlerLast(new ReadTimeoutHandler(3, TimeUnit.SECONDS))
                            .addHandlerLast(new WriteTimeoutHandler(3, TimeUnit.SECONDS)));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .filter(retryFilter())
                .build();
    }

    private ExchangeFilterFunction retryFilter() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            if (clientResponse.statusCode().is5xxServerError()) {
                log.warn("Received 5xx error from external API, will retry");
                return Mono.error(new RuntimeException("Server error"));
            }
            return Mono.just(clientResponse);
        });
    }
}
