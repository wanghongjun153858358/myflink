package com.red.stream.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

/**
 * 商品信息
 *
 * @author red
 * @create 2019-06-24-10:16
 */
@Data
@ToString
public class Product {
    private Integer productId;
    private double price;
    private Integer amount;

}
