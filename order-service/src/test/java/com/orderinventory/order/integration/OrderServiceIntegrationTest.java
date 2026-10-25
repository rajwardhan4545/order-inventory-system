package com.orderinventory.order.integration;

import com.orderinventory.order.dto.CreateOrderRequest;
import com.orderinventory.order.dto.OrderDetailsResponse;
import com.orderinventory.order.dto.OrderResponse;
import com.orderinventory.order.entity.OrderStatus;
import com.orderinventory.order.exception.OrderNotFoundException;
import com.orderinventory.order.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for OrderService using H2 in-memory database and EmbeddedKafka.
 * These tests exercise the real Spring context, JPA persistence, and Kafka publishing.
 */
@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.ANY)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-placed", "order-cancelled", "inventory-updated", "order-out-of-stock"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0"
        }
)
@TestPropertySource(locations = "classpath:application-test.properties")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderServiceIntegrationTest {

    @Autowired
    private OrderService orderService;

    // -----------------------------------------------------------------------
    // Test 1: Place an order and assert status is PENDING
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("createOrder should persist order with PENDING status")
    void testCreateOrder_StatusIsPending() {
        // Arrange
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId(101L)
                .productId(55L)
                .quantity(2)
                .build();

        // Act
        OrderResponse response = orderService.createOrder(request);

        // Assert
        assertNotNull(response, "Response must not be null");
        assertNotNull(response.getOrderId(), "Order ID must not be null after save");
        assertEquals(OrderStatus.PENDING, response.getStatus(),
                "Newly created order must have status PENDING");
        assertEquals("Order received, awaiting inventory confirmation", response.getMessage());
    }

    // -----------------------------------------------------------------------
    // Test 2: Fetch non-existent order -> OrderNotFoundException
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("getOrderById with non-existent ID should throw OrderNotFoundException")
    void testGetOrderById_NonExistent_ThrowsException() {
        long nonExistentId = 99999L;

        assertThrows(
                OrderNotFoundException.class,
                () -> orderService.getOrderById(nonExistentId),
                "Expected OrderNotFoundException for non-existent order ID"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: Cancel a PENDING order -> status becomes CANCELLED
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("cancelOrder on PENDING order should set status to CANCELLED")
    void testCancelOrder_PendingOrder_StatusCancelled() {
        // Arrange: first create an order
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId(102L)
                .productId(56L)
                .quantity(1)
                .build();
        OrderResponse created = orderService.createOrder(request);
        Long orderId = created.getOrderId();
        assertNotNull(orderId, "Created order must have an ID");

        // Act: cancel the order
        OrderDetailsResponse cancelled = orderService.cancelOrder(orderId);

        // Assert
        assertNotNull(cancelled, "Cancelled order response must not be null");
        assertEquals(OrderStatus.CANCELLED, cancelled.getStatus(),
                "Cancelled order must have status CANCELLED");
        assertEquals(orderId, cancelled.getId(),
                "Cancelled order ID must match the created order ID");
    }
}
