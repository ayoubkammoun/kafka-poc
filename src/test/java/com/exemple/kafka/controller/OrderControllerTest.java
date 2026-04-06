package com.exemple.kafka.controller;

import com.exemple.kafka.model.Order;
import com.exemple.kafka.producer.OrderProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderProducer orderProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createOrder_shouldReturnAccepted() throws Exception {
        Order order = orderWithoutId();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isAccepted());

        verify(orderProducer).sendOrder(any(Order.class));
    }

    @Test
    void createOrder_shouldGenerateOrderIdWhenAbsent() throws Exception {
        Order order = orderWithoutId();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").isNotEmpty());
    }

    @Test
    void createOrder_shouldDefaultStatusToPending() throws Exception {
        Order order = orderWithoutId();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void createOrder_shouldPreserveProvidedOrderId() throws Exception {
        Order order = Order.builder()
                .orderId("my-fixed-order-id")
                .customerId("cust-1")
                .product("Widget")
                .quantity(1)
                .price(BigDecimal.valueOf(9.99))
                .status(Order.OrderStatus.CONFIRMED)
                .build();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("my-fixed-order-id"));
    }

    @Test
    void createSampleOrder_shouldReturnAcceptedWithLaptop() throws Exception {
        mockMvc.perform(post("/api/orders/sample"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.product").value("Laptop"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.orderId").isNotEmpty());

        verify(orderProducer).sendOrder(any(Order.class));
    }

    private Order orderWithoutId() {
        return Order.builder()
                .customerId("cust-1")
                .product("Widget")
                .quantity(2)
                .price(BigDecimal.valueOf(19.99))
                .build();
    }
}
