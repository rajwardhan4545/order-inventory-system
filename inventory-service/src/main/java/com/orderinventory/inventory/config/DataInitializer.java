package com.orderinventory.inventory.config;

import com.orderinventory.inventory.entity.Product;
import com.orderinventory.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

@Slf4j
@Configuration
@Profile("!test")
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        if (productRepository.count() == 0) {
            log.info("Populating initial demo products into Inventory Database...");

            Product p1 = Product.builder()
                    .id(55L)
                    .name("Wireless Mechanical Keyboard")
                    .availableQuantity(50)
                    .reservedQuantity(0)
                    .version(0L)
                    .build();

            Product p2 = Product.builder()
                    .id(56L)
                    .name("Ergonomic Vertical Mouse")
                    .availableQuantity(25)
                    .reservedQuantity(0)
                    .version(0L)
                    .build();

            Product p3 = Product.builder()
                    .id(57L)
                    .name("4K Ultra HD Gaming Monitor")
                    .availableQuantity(10)
                    .reservedQuantity(0)
                    .version(0L)
                    .build();

            Product p4 = Product.builder()
                    .id(58L)
                    .name("USB-C Multiport Hub")
                    .availableQuantity(1)
                    .reservedQuantity(0)
                    .version(0L)
                    .build();

            productRepository.saveAll(List.of(p1, p2, p3, p4));
            log.info("Demo products initialized successfully: {}", productRepository.findAll());
        }
    }
}
