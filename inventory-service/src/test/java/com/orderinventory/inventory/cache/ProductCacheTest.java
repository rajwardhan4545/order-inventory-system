package com.orderinventory.inventory.cache;

import com.orderinventory.inventory.config.RedisConfig;
import com.orderinventory.inventory.dto.ProductResponse;
import com.orderinventory.inventory.dto.StockUpdateRequest;
import com.orderinventory.inventory.dto.event.OrderCancelledEvent;
import com.orderinventory.inventory.dto.event.OrderPlacedEvent;
import com.orderinventory.inventory.entity.Product;
import com.orderinventory.inventory.kafka.InventoryProducer;
import com.orderinventory.inventory.repository.ProductRepository;
import com.orderinventory.inventory.service.InventoryService;
import com.orderinventory.inventory.service.InventoryServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {InventoryServiceImpl.class, RedisConfig.class})
@ActiveProfiles("test")
@TestPropertySource(locations = "classpath:application-test.properties")
class ProductCacheTest {

    @MockBean
    private ProductRepository productRepository;

    @MockBean
    private InventoryProducer inventoryProducer;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private CacheManager cacheManager;

    private Product testProduct;

    @BeforeEach
    void setUp() {
        testProduct = Product.builder()
                .id(1L)
                .name("Cached Laptop")
                .availableQuantity(50)
                .reservedQuantity(5)
                .updatedAt(LocalDateTime.now())
                .build();

        Cache cache = cacheManager.getCache("products");
        if (cache != null) {
            cache.clear();
        }
    }

    @Test
    @DisplayName("getProductById should hit repository once and subsequent call served from cache")
    void testGetProductById_Cached() {
        when(productRepository.findById(1L)).thenReturn(Optional.of(testProduct));

        // First call - should invoke repo and populate cache
        ProductResponse res1 = inventoryService.getProductById(1L);
        assertNotNull(res1);
        assertEquals("Cached Laptop", res1.getName());
        verify(productRepository, times(1)).findById(1L);

        // Second call - should return from cache, repository not invoked again
        ProductResponse res2 = inventoryService.getProductById(1L);
        assertNotNull(res2);
        assertEquals("Cached Laptop", res2.getName());
        verify(productRepository, times(1)).findById(1L);
    }

    @Test
    @DisplayName("getAllProducts should cache results on first invocation")
    void testGetAllProducts_Cached() {
        when(productRepository.findAll()).thenReturn(List.of(testProduct));

        List<ProductResponse> list1 = inventoryService.getAllProducts();
        assertEquals(1, list1.size());
        verify(productRepository, times(1)).findAll();

        List<ProductResponse> list2 = inventoryService.getAllProducts();
        assertEquals(1, list2.size());
        verify(productRepository, times(1)).findAll();
    }

    @Test
    @DisplayName("updateStock should evict product entry and all-products list from cache")
    void testUpdateStock_EvictsCache() {
        when(productRepository.findById(1L)).thenReturn(Optional.of(testProduct));
        when(productRepository.save(any(Product.class))).thenReturn(testProduct);

        // Populate cache
        inventoryService.getProductById(1L);
        Cache cache = cacheManager.getCache("products");
        assertNotNull(cache);
        assertNotNull(cache.get(1L));

        // Perform stock update -> triggers @CacheEvict
        StockUpdateRequest updateRequest = new StockUpdateRequest(40, 5);
        inventoryService.updateStock(1L, updateRequest);

        // Cache for product 1L should be null (evicted)
        assertNull(cache.get(1L));
    }

    @Test
    @DisplayName("processOrderPlaced should evict product entry from cache")
    void testProcessOrderPlaced_EvictsCache() {
        when(productRepository.findByIdForUpdate(1L)).thenReturn(Optional.of(testProduct));
        when(productRepository.save(any(Product.class))).thenReturn(testProduct);

        // Populate cache
        Cache cache = cacheManager.getCache("products");
        assertNotNull(cache);
        cache.put(1L, ProductResponse.fromEntity(testProduct));
        assertNotNull(cache.get(1L));

        // Process order placed event -> triggers evict
        OrderPlacedEvent event = OrderPlacedEvent.builder()
                .orderId(101L)
                .productId(1L)
                .quantity(2)
                .build();
        inventoryService.processOrderPlaced(event);

        assertNull(cache.get(1L));
    }

    @Test
    @DisplayName("processOrderCancelled should evict product entry from cache")
    void testProcessOrderCancelled_EvictsCache() {
        when(productRepository.releaseStock(1L, 2)).thenReturn(1);

        Cache cache = cacheManager.getCache("products");
        assertNotNull(cache);
        cache.put(1L, ProductResponse.fromEntity(testProduct));
        assertNotNull(cache.get(1L));

        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .orderId(101L)
                .productId(1L)
                .quantity(2)
                .build();
        inventoryService.processOrderCancelled(event);

        assertNull(cache.get(1L));
    }
}
