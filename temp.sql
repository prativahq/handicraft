USE test_db;
CREATE TABLE `7903_wc_customer_lookup` (
    `customer_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id` BIGINT(20) UNSIGNED DEFAULT NULL,
    `username` VARCHAR(60) NOT NULL,
    `first_name` VARCHAR(255) NOT NULL DEFAULT '',
    `last_name` VARCHAR(255) NOT NULL DEFAULT '',
    `email` VARCHAR(100) DEFAULT NULL,
    `date_last_active` TIMESTAMP NULL DEFAULT NULL,
    `date_registered` TIMESTAMP NULL DEFAULT NULL,
    `country` CHAR(2) NOT NULL,
    `postcode` VARCHAR(20) NOT NULL,
    `city` VARCHAR(100) NOT NULL,
    `state` VARCHAR(100) NOT NULL,
    PRIMARY KEY (`customer_id`),
    UNIQUE KEY `user_id` (`user_id`),
    KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



-- Sample INSERT statements for 7903_wc_customer_lookup

INSERT INTO `7903_wc_customer_lookup` 
(user_id, username, first_name, last_name, email, date_last_active, date_registered, country, postcode, city, state) 
VALUES 
(1001, 'john_doe', 'John', 'Doe', 'john@example.com', '2024-03-15 10:30:00', '2023-01-01 08:00:00', 'US', '10001', 'New York', 'NY');

INSERT INTO `7903_wc_customer_lookup` 
(user_id, username, first_name, last_name, email, date_last_active, date_registered, country, postcode, city, state) 
VALUES 
(1002, 'jane_smith', 'Jane', 'Smith', 'jane@example.com', '2024-03-14 15:45:00', '2023-02-15 09:30:00', 'UK', 'SW1A 1AA', 'London', 'England');

INSERT INTO `7903_wc_customer_lookup` 
(user_id, username, first_name, last_name, email, date_last_active, date_registered, country, postcode, city, state) 
VALUES 
(1003, 'mike_wilson', 'Mike', 'Wilson', 'mike@example.com', '2024-03-13 12:15:00', '2023-03-20 14:20:00', 'CA', 'M5V 2T6', 'Toronto', 'Ontario');

-- setup 7903_wc_customer_lookup_trigger
BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.customer_id,
        NOW(),
        'INSERT',
        '7903_wc_customer_lookup',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.order_id,
        NOW(),
        'INSERT',
        '7903_wc_order_stats',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.order_id,
        NOW(),
        'INSERT',
        '7903_wc_order_product_lookup',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.term_id,
        NOW(),
        'INSERT',
        '7903_term_taxonomy',
        0
    );
END


BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.ID,
        NOW(),
        'INSERT',
        '7903_posts',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.ID,
        NOW(),
        'UPDATE',
        '7903_posts',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.order_item_id,
        NOW(),
        'UPDATE',
        '7903_woocommerce_order_itemmeta',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.term_id,
        NOW(),
        'UPDATE',
        '7903_term_taxonomy',
        0
    );
END

BEGIN
    INSERT INTO `trigger_table`
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.customer_id,        -- ID of the updated customer
        NOW(),                  -- Current timestamp
        'UPDATE',               -- Operation type
        '7903_wc_customer_lookup', -- Name of the affected table
        0                       -- Default processing status
    );

END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.order_item_id,
        NOW(),
        'INSERT',
        '7903_woocommerce_order_items',
        0
    );
END

BEGIN
    INSERT INTO trigger_table
    (
        id,
        created_at,
        operation,
        table_name,
        is_processed
    )
    VALUES
    (
        NEW.order_item_id,
        NOW(),
        'UPDATE',
        '7903_woocommerce_order_items',
        0
    );
END

-- setup trigegr table
USE test_db;

CREATE TABLE `trigger_table` (
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `created_at` DATE NOT NULL,
    `operation` VARCHAR(100) NOT NULL,
    `table_name` VARCHAR(100) NOT NULL,
    `is_processed` TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;




CREATE TABLE `7903_usermeta` (
    `umeta_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
    `meta_key` varchar(255) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
    `meta_value` longtext COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
    PRIMARY KEY (`umeta_id`),
    KEY `user_id` (`user_id`),
    KEY `meta_key` (`meta_key`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

-- Sample data inserts
INSERT INTO `7903_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES
(1001, 'billing_phone', '+1-555-0123'),
(1001, 'billing_address', '123 Main St'),
(1002, 'billing_phone', '+1-555-4567'),
(1003, 'shipping_address', '789 Oak Road');