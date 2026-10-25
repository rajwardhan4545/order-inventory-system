package com.orderinventory.inventory.integration;

import com.orderinventory.inventory.dto.ProductRequest;
import com.orderinventory.inventory.dto.ProductResponse;
import com.orderinventory.inventory.dto.StockUpdateRequest;
import com.orderinventory.inventory.dto.event.OrderPlacedEvent;
import com.orderinventory.inventory.exception.ProductNotFoundException;
import com.orderinventory.inventory.service.InventoryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for InventoryService using H2 in-memory database and EmbeddedKafka.
 * These tests exercise real Spring context, JPA persistence, and service logic end-to-end.
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
class InventoryServiceIntegrationTest {

    @Autowired
    private InventoryService inventoryService;

    // -----------------------------------------------------------------------
    // Helper: add a product and return its saved response
    // -----------------------------------------------------------------------
    private ProductResponse addTestProduct(String name, int availableQty) {
        ProductRequest request = ProductRequest.builder()
                .name(name)
                .availableQuantity(availableQty)
                .reservedQuantity(0)
                .build();
        return inventoryService.addProduct(request);
    }

    // -----------------------------------------------------------------------
    // Test 1: Add product and retrieve it by ID
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("addProduct and getProductById should persist and return the product")
    void testAddProduct_ThenGetById_ReturnsProduct() {
        // Act
        ProductResponse saved = addTestProduct("Integration Test Widget", 100);

        assertNotNull(saved.getId(), "Saved product must have a generated ID");

        ProductResponse fetched = inventoryService.getProductById(saved.getId());

        // Assert
        assertNotNull(fetched, "Fetched product must not be null");
        assertEquals("Integration Test Widget", fetched.getName());
        assertEquals(100, fetched.getAvailableQuantity());
        assertEquals(0, fetched.getReservedQuantity());
    }

    // -----------------------------------------------------------------------
    // Test 2: Get non-existent product throws ProductNotFoundException
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("getProductById with non-existent ID should throw ProductNotFoundException")
    void testGetProductById_NonExistent_ThrowsException() {
        long nonExistentId = 88888L;

        assertThrows(
                ProductNotFoundException.class,
                () -> inventoryService.getProductById(nonExistentId),
                "Expected ProductNotFoundException for non-existent product ID"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: updateStock changes available and reserved quantities
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("updateStock should persist new stock quantities")
    void testUpdateStock_UpdatesQuantities() {
        // Arrange: create a product
        ProductResponse saved = addTestProduct("Stock Update Test Product", 50);
        Long productId = saved.getId();

        StockUpdateRequest updateRequest = StockUpdateRequest.builder()
                .availableQuantity(75)
                .reservedQuantity(10)
                .build();

        // Act
        ProductResponse updated = inventoryService.updateStock(productId, updateRequest);

        // Assert
        assertNotNull(updated, "Updated product response must not be null");
        assertEquals(75, updated.getAvailableQuantity(),
                "Available quantity must be updated to 75");
        assertEquals(10, updated.getReservedQuantity(),
                "Reserved quantity must be updated to 10");
    }

    // -----------------------------------------------------------------------
    // Test 4: getAllProducts returns the products that were added
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("getAllProducts should return at least the products added in this test")
    void testGetAllProducts_ReturnsSavedProducts() {
        // Arrange: add two products
        addTestProduct("Product Alpha", 30);
        addTestProduct("Product Beta", 20);

        // Act
        List<ProductResponse> products = inventoryService.getAllProducts();

        // Assert: at least these 2 products exist (DataInitializer may add more)
        assertNotNull(products, "Product list must not be null");
        assertTrue(products.size() >= 2,
                "Expected at least 2 products, but got: " + products.size());
    }

    // -----------------------------------------------------------------------
    // Test 5: Concurrency / Overselling Test - 10 concurrent orders on 2 units
    // -----------------------------------------------------------------------
    @Test
    @DisplayName("CONCURRENCY TEST: 10 simultaneous orders competing for 2 available units")
    void testConcurrentOrdersCompetingForStock_PreventsOverselling() throws Exception {
        // Arrange: Create product with stock = 2
        ProductResponse product = addTestProduct("Limited Edition Smartphone", 2);
        Long productId = product.getId();

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numThreads);

        for (int i = 1; i <= numThreads; i++) {
            final long orderId = 1000L + i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Guarantee simultaneous thread launch
                    OrderPlacedEvent event = OrderPlacedEvent.builder()
                            .orderId(orderId)
                            .productId(productId)
                            .quantity(1)
                            .build();

                    inventoryService.processOrderPlaced(event);
                } catch (Exception e) {
                    System.err.println("Thread error: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Act: Unleash all 10 threads concurrently
        startLatch.countDown();
        boolean completed = finishLatch.await(15, TimeUnit.SECONDS);
        assertTrue(completed, "All 10 concurrent threads must complete within 15 seconds");
        executor.shutdown();

        // Assert: Inspect database state
        ProductResponse finalProduct = inventoryService.getProductById(productId);

        System.out.println("=================================================================");
        System.out.println(">>> CONCURRENCY / OVERSELLING MEASUREMENT RESULTS <<<");
        System.out.println("Initial Stock Available: 2");
        System.out.println("Simultaneous Threads Fired: " + numThreads);
        System.out.println("Final Stock Available: " + finalProduct.getAvailableQuantity());
        System.out.println("Final Stock Reserved: " + finalProduct.getReservedQuantity());
        System.out.println("Confirmed Orders: " + finalProduct.getReservedQuantity() + " (Expected: 2)");
        System.out.println("Out of Stock Rejections: " + (numThreads - finalProduct.getReservedQuantity()) + " (Expected: 8)");
        System.out.println("Overselling Detected: " + (finalProduct.getAvailableQuantity() < 0 ? "YES (BUG!)" : "NO (ZERO OVERSELLING)"));
        System.out.println("=================================================================");

        assertEquals(0, finalProduct.getAvailableQuantity(), "Available stock must be exactly 0 (no overselling)");
        assertEquals(2, finalProduct.getReservedQuantity(), "Reserved stock must be exactly 2 (only 2 orders succeeded)");
    }
}
