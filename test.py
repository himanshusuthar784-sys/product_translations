"""

Original Products Structure:

    CREATE TABLE `shop_products_1001_test` (
    `id` int(11) NOT NULL,
    `shop_id` int(10) UNSIGNED NOT NULL,
    `product_id` bigint(20) UNSIGNED NOT NULL,
    `title` text NOT NULL,
    `variants_count` int(11) NOT NULL DEFAULT 0,
    `meta_title` text DEFAULT NULL,
    `handle` text NOT NULL,
    `brand` varchar(255) NOT NULL,
    `gender` varchar(30) NOT NULL,
    `age_group` varchar(30) NOT NULL,
    `tags` text DEFAULT NULL,
    `google_product_category` text NOT NULL,
    `google_status` tinyint(1) NOT NULL DEFAULT 0,
    `is_available_for_google` tinyint(1) NOT NULL DEFAULT 1,
    `is_available_for_google_display_ads` tinyint(1) NOT NULL DEFAULT 1,
    `is_available_for_google_shopping_actions` tinyint(4) NOT NULL DEFAULT 1,
    `is_available_for_bing` tinyint(1) NOT NULL DEFAULT 1,
    `is_available_for_facebook` tinyint(1) NOT NULL DEFAULT 1,
    `is_available_for_xml` tinyint(1) NOT NULL DEFAULT 1,
    `is_deleted` tinyint(1) NOT NULL DEFAULT 0,
    `created_at` timestamp NULL DEFAULT current_timestamp(),
    `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;


Updated structure (We have move the fields that are translatable from original product table to seperate product translations table)
Products:
        `id` int(11) NOT NULL,
        `shop_id` int(10) UNSIGNED NOT NULL,
        `product_id` bigint(20) UNSIGNED NOT NULL,
        `variants_count` int(11) NOT NULL DEFAULT 0,
        `handle` text NOT NULL,
        `gender` varchar(30) NOT NULL,
        `age_group` varchar(30) NOT NULL,
        `google_status` tinyint(1) NOT NULL DEFAULT 0,
        `is_available_for_google` tinyint(1) NOT NULL DEFAULT 1,
        `is_available_for_google_display_ads` tinyint(1) NOT NULL DEFAULT 1,
        `is_available_for_google_shopping_actions` tinyint(4) NOT NULL DEFAULT 1,
        `is_available_for_bing` tinyint(1) NOT NULL DEFAULT 1,
        `is_available_for_facebook` tinyint(1) NOT NULL DEFAULT 1,
        `is_available_for_xml` tinyint(1) NOT NULL DEFAULT 1,
        `is_deleted` tinyint(1) NOT NULL DEFAULT 0,
        `created_at` timestamp NULL DEFAULT current_timestamp(),
        `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
        
        - unique (shop_id, product_id)
        - idx (shop_id, product_id, created_at desc, id desc)

    langauges
        id, name, language_code, created_at, updated_at
        idx: language_code

    Option1:
        product_translations
            id
            product_id: FK (products)
            language_id: FK (languages)
            key: TEXT (NOT NULL) (can be title, desscription, meta_title, category, brand, etc)
            value: JSONB ({"value": Any})
            
            - unique_(product_id, language_id, key)



    FastAPI
        - Migration Utility (Covers mulkti language supports)
        - Background jobs (celery + redis) -> Workers (stateless) -> Use existing redis from docker. 
        - API for monitoring progress by polling

Test sql: C:\Users\LENOVO\Desktop\SI\shop_products_1001_test.sql.mhtml



Redis Queue:
    -> queue depth (jobs) metric
    -> Auto scale
    
    -> memory/cpu
        > 70% -> scale workers
        > 80% -> scale workers
        
-> 8vCPU, 50GB RAM, 500GB disk
    -> default (4 workers)
        -> min (3), max (8)
    -> scale

ECS -> worker




"""