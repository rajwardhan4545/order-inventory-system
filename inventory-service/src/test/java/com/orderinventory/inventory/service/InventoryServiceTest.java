package com.orderinventory.inventory.service;

import com.orderinventory.inventory.dto.ProductRequest;
import com.orderinventory.inventory.dto.ProductResponse;
import com.orderinventory.inventory.dto.StockUpdateRequest;
import com.orderinventory.inventory.dto.event.InventoryUpdatedEvent;
import com.orderinventory.inventory.dto.event.OrderOutOfStockEvent;
import com.orderinventory.inventory.dto.event.OrderPlacedEvent;
import com.orderinventory.inventory.entity.Product;
import com.orderinventory.inventory.exception.ProductNotFoundException;
import com.orderinventory.inventory.kafka.InventoryProducer;
import com.orderinventory.inventory.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private InventoryProducer inventoryProducer;

    @InjectMocks
    private InventoryServiceImpl inventoryService;

    private Product sampleProduct;

    @BeforeEach
    void setUp() {
        sampleProduct = Product.builder()
                .id(55L)
                .name("Wireless Mechanical Keyboard")
                .availableQuantity(50)
                .reservedQuantity(0)
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Test
    void testGetAllProducts() {
        when(productRepository.findAll()).thenReturn(List.of(sampleProduct));

        List<ProductResponse> products = inventoryService.getAllProducts();

        assertNotNull(products);
        assertEquals(1, products.size());
        assertEquals(55L, products.get(0).getId());
        assertEquals("Wireless Mechanical Keyboard", products.get(0).getName());
    }

    @Test
    void testGetProductById_Success() {
        when(productRepository.findById(55L)).thenReturn(Optional.of(sampleProduct));

        ProductResponse response = inventoryService.getProductById(55L);

        assertNotNull(response);
        assertEquals(55L, response.getId());
        assertEquals(50, response.getAvailableQuantity());
    }

    @Test
    void testGetProductById_NotFound() {
        when(productRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(ProductNotFoundException.class, () -> inventoryService.getProductById(999L));
    }

    @Test
    void testAddProduct_Success() {
        ProductRequest request = ProductRequest.builder()
                .id(55L)
                .name("Wireless Mechanical Keyboard")
                .availableQuantity(50)
                .reservedQuantity(0)
                .build();

        when(productRepository.save(any(Product.class))).thenReturn(sampleProduct);

        ProductResponse response = inventoryService.addProduct(request);

        assertNotNull(response);
        assertEquals(55L, response.getId());
        verify(productRepository, times(1)).save(any(Product.class));
    }

    @Test
    void testUpdateStock_Success() {
        StockUpdateRequest request = StockUpdateRequest.builder()
                .availableQuantity(75)
                .reservedQuantity(5)
                .build();

        when(productRepository.findById(55L)).thenReturn(Optional.of(sampleProduct));
        when(productRepository.save(any(Product.class))).thenAnswer(invocation -> invocation.getArgument(0));

        ProductResponse response = inventoryService.updateStock(55L, request);

        assertNotNull(response);
        assertEquals(75, response.getAvailableQuantity());
        assertEquals(5, response.getReservedQuantity());
    }

    @Test
    void testProcessOrderPlaced_SufficientStock() {
        OrderPlacedEvent event = OrderPlacedEvent.builder()
                .orderId(1001L)
                .productId(55L)
                .quantity(2)
                .customerId(101L)
                .timestamp(LocalDateTime.now())
                .build();

        when(productRepository.findByIdForUpdate(55L)).thenReturn(Optional.of(sampleProduct));
        when(productRepository.save(any(Product.class))).thenAnswer(invocation -> invocation.getArgument(0));
        doNothing().when(inventoryProducer).publishInventoryUpdated(any(InventoryUpdatedEvent.class));

        inventoryService.processOrderPlaced(event);

        assertEquals(48, sampleProduct.getAvailableQuantity());
        assertEquals(2, sampleProduct.getReservedQuantity());

        ArgumentCaptor<InventoryUpdatedEvent> captor = ArgumentCaptor.forClass(InventoryUpdatedEvent.class);
        verify(inventoryProducer, times(1)).publishInventoryUpdated(captor.capture());
        assertEquals(1001L, captor.getValue().getOrderId());
        assertEquals("CONFIRMED", captor.getValue().getStatus());
        assertEquals(48, captor.getValue().getRemainingStock());
    }

    @Test
    void testProcessOrderPlaced_InsufficientStock() {
        OrderPlacedEvent event = OrderPlacedEvent.builder()
                .orderId(1002L)
                .productId(55L)
                .quantity(100) // requesting 100 when only 50 available
                .customerId(101L)
                .timestamp(LocalDateTime.now())
                .build();

        when(productRepository.findByIdForUpdate(55L)).thenReturn(Optional.of(sampleProduct));
        doNothing().when(inventoryProducer).publishOrderOutOfStock(any(OrderOutOfStockEvent.class));

        inventoryService.processOrderPlaced(event);

        // Quantities should remain unchanged
        assertEquals(50, sampleProduct.getAvailableQuantity());
        assertEquals(0, sampleProduct.getReservedQuantity());

        ArgumentCaptor<OrderOutOfStockEvent> captor = ArgumentCaptor.forClass(OrderOutOfStockEvent.class);
        verify(inventoryProducer, times(1)).publishOrderOutOfStock(captor.capture());
        assertEquals(1002L, captor.getValue().getOrderId());
        assertEquals("OUT_OF_STOCK", captor.getValue().getStatus());
        assertEquals("Insufficient stock", captor.getValue().getReason());
    }
}
