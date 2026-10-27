package com.orderinventory.inventory.service;

import com.orderinventory.inventory.dto.ProductRequest;
import com.orderinventory.inventory.dto.ProductResponse;
import com.orderinventory.inventory.dto.StockUpdateRequest;
import com.orderinventory.inventory.dto.event.InventoryUpdatedEvent;
import com.orderinventory.inventory.dto.event.OrderCancelledEvent;
import com.orderinventory.inventory.dto.event.OrderOutOfStockEvent;
import com.orderinventory.inventory.dto.event.OrderPlacedEvent;
import com.orderinventory.inventory.entity.Product;
import com.orderinventory.inventory.exception.ProductNotFoundException;
import com.orderinventory.inventory.kafka.InventoryProducer;
import com.orderinventory.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryServiceImpl implements InventoryService {

    private final ProductRepository productRepository;
    private final InventoryProducer inventoryProducer;

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "products", key = "'all'")
    public List<ProductResponse> getAllProducts() {
        log.info("Fetching all products from inventory (Cache Miss)");
        return productRepository.findAll()
                .stream()
                .map(ProductResponse::fromEntity)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "products", key = "#id")
    public ProductResponse getProductById(Long id) {
        log.info("Fetching product with ID: {} (Cache Miss)", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + id));
        return ProductResponse.fromEntity(product);
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = "products", key = "'all'")
    }, put = {
        @CachePut(value = "products", key = "#result.id")
    })
    public ProductResponse addProduct(ProductRequest request) {
        log.info("Adding new product: {}", request.getName());

        Product.ProductBuilder builder = Product.builder()
                .name(request.getName())
                .availableQuantity(request.getAvailableQuantity())
                .reservedQuantity(request.getReservedQuantity() != null ? request.getReservedQuantity() : 0);

        if (request.getId() != null) {
            builder.id(request.getId());
        }

        Product product = builder.build();
        Product savedProduct = productRepository.save(product);
        log.info("Product created with ID: {}", savedProduct.getId());
        return ProductResponse.fromEntity(savedProduct);
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = "products", key = "#id"),
        @CacheEvict(value = "products", key = "'all'")
    })
    public ProductResponse updateStock(Long id, StockUpdateRequest request) {
        log.info("Updating stock for product ID: {}", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + id));

        product.setAvailableQuantity(request.getAvailableQuantity());
        if (request.getReservedQuantity() != null) {
            product.setReservedQuantity(request.getReservedQuantity());
        }

        Product updatedProduct = productRepository.save(product);
        log.info("Stock updated for product ID: {}, new available: {}", id, updatedProduct.getAvailableQuantity());
        return ProductResponse.fromEntity(updatedProduct);
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = "products", key = "#event.productId"),
        @CacheEvict(value = "products", key = "'all'")
    })
    public void processOrderPlaced(OrderPlacedEvent event) {
        log.info("Processing OrderPlacedEvent: orderId={}, productId={}, quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());

        Optional<Product> productOpt = productRepository.findByIdForUpdate(event.getProductId());

        if (productOpt.isEmpty()) {
            log.warn("Product not found with ID: {}. Publishing OrderOutOfStockEvent for orderId: {}",
                    event.getProductId(), event.getOrderId());
            
            OrderOutOfStockEvent outOfStockEvent = OrderOutOfStockEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .status("OUT_OF_STOCK")
                    .reason("Product not found with id: " + event.getProductId())
                    .build();
            inventoryProducer.publishOrderOutOfStock(outOfStockEvent);
            return;
        }

        Product product = productOpt.get();

        if (product.getAvailableQuantity() >= event.getQuantity()) {
            // Sufficient stock available -> deduct and reserve
            int newAvailable = product.getAvailableQuantity() - event.getQuantity();
            int newReserved = product.getReservedQuantity() + event.getQuantity();
            product.setAvailableQuantity(newAvailable);
            product.setReservedQuantity(newReserved);
            productRepository.save(product);

            log.info("Stock deducted successfully for product ID: {}. Remaining available: {}, Reserved: {}",
                    product.getId(), newAvailable, newReserved);

            InventoryUpdatedEvent updatedEvent = InventoryUpdatedEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(product.getId())
                    .status("CONFIRMED")
                    .remainingStock(newAvailable)
                    .build();
            inventoryProducer.publishInventoryUpdated(updatedEvent);
        } else {
            // Insufficient stock -> publish out of stock event
            log.warn("Insufficient stock for product ID: {}. Requested: {}, Available: {}",
                    product.getId(), event.getQuantity(), product.getAvailableQuantity());

            OrderOutOfStockEvent outOfStockEvent = OrderOutOfStockEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(product.getId())
                    .status("OUT_OF_STOCK")
                    .reason("Insufficient stock")
                    .build();
            inventoryProducer.publishOrderOutOfStock(outOfStockEvent);
        }
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = "products", key = "#event.productId"),
        @CacheEvict(value = "products", key = "'all'")
    })
    public void processOrderCancelled(OrderCancelledEvent event) {
        log.info("Processing OrderCancelledEvent: orderId={}, productId={}, quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());

        int rowsUpdated = productRepository.releaseStock(event.getProductId(), event.getQuantity());

        if (rowsUpdated > 0) {
            log.info("Successfully released {} units of reserved stock for product ID: {} (order: {})",
                    event.getQuantity(), event.getProductId(), event.getOrderId());
        } else {
            log.warn("Failed to release stock for product ID: {}. Product not found or insufficient reserved quantity. Order: {}",
                    event.getProductId(), event.getOrderId());
        }
    }
}
