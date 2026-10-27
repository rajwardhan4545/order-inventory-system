package com.orderinventory.inventory.dto;

import com.orderinventory.inventory.entity.Product;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private String name;
    private Integer availableQuantity;
    private Integer reservedQuantity;
    private LocalDateTime updatedAt;

    public static ProductResponse fromEntity(Product product) {
        return ProductResponse.builder()
                .id(product.getId())
                .name(product.getName())
                .availableQuantity(product.getAvailableQuantity())
                .reservedQuantity(product.getReservedQuantity())
                .updatedAt(product.getUpdatedAt())
                .build();
    }
}
