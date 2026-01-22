SELECT DISTINCT
                parent.entity_id AS Parent_ID,
                parent.sku AS Parent_SKU,
                pname.value AS Parent_Name,
                parent_url_key.value AS Parent_URLKey,
                parent.updated_at AS Parent_UpdatedAt,
                pstatus.value AS Parent_Status,
                child.entity_id AS Child_ID,
                child.sku AS Child_SKU,
                cname.value AS Child_Name,
                child_url_key.value AS Child_URLKey,
                child_url_key.store_id AS Child_Store,
                child_special_from.value AS Child_Special_From_Date,
                child_special_to.value AS Child_Special_To_Date,
                    child.match_score AS Child_MatchScore 
                FROM catalog_product_link l
                    JOIN (
                    SELECT
                        *,
                        (
			(LENGTH(c.body) - LENGTH(REPLACE(c.body, 'oli', ''))) / 3 + 
			(LENGTH(c.body) - LENGTH(REPLACE(c.body, 'liv', ''))) / 3 + 
			(LENGTH(c.body) - LENGTH(REPLACE(c.body, 'iva', ''))) / 3) AS match_score
                    FROM (
                        SELECT
                            *,
                            LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,
                            LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body
                        FROM catalog_product_entity
                    ) c
                ) child ON child.entity_id = l.linked_product_id
                    JOIN catalog_product_entity parent
                        ON parent.entity_id = l.product_id
                        AND parent.type_id = 'grouped'
                    JOIN catalog_product_entity_int pstatus
                        ON pstatus.entity_id = parent.entity_id
                        AND pstatus.attribute_id = 89
                        AND pstatus.store_id = 0
                        AND pstatus.value = 1
                    JOIN catalog_product_entity_varchar parent_url_key
                        ON parent_url_key.entity_id = parent.entity_id
                        AND parent_url_key.attribute_id = 90
                        AND parent_url_key.store_id = 0
                    JOIN catalog_product_entity_varchar pname
                        ON pname.entity_id = parent.entity_id
                        AND pname.attribute_id = 65
                        AND pname.store_id = 0
                    JOIN catalog_product_entity_varchar child_url_key
                        ON child_url_key.entity_id = child.entity_id
                        AND child_url_key.attribute_id = 90
                        AND child_url_key.store_id IN (6,8,9)
                    JOIN catalog_product_entity_varchar cname
                        ON cname.entity_id = child.entity_id
                        AND cname.store_id = child_url_key.store_id
                        AND cname.attribute_id = 65
                    JOIN catalog_product_entity_datetime child_special_from
                        ON child_special_from.entity_id = child.entity_id
                        AND child_special_from.store_id = child_url_key.store_id
                        AND child_special_from.attribute_id = 71
                        AND child_special_from.value < NOW()
                    JOIN catalog_product_entity_datetime child_special_to
                        ON child_special_to.entity_id = child.entity_id
                        AND child_special_to.store_id = child_url_key.store_id
                        AND child_special_to.attribute_id = 72
                        AND child_special_to.value > NOW()
                    WHERE l.link_type_id = 3
                        AND parent.sku REGEXP '[0-9]{8}'
                    AND child.prefix = 'k1'
                    AND child.match_score > 0
                        ORDER BY child.match_score DESC
                        LIMIT 100