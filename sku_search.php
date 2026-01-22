<?php

class TestDealSearch {
    protected function _buildDealSearchQuery($options)
    {
        $childColumns = isset($options['childColumns']) ? $options['childColumns'] : array();
        $matchScoreExpr = isset($options['matchScoreExpr']) ? $options['matchScoreExpr'] : '';
        $additionalChildSubqueryColumns = isset($options['additionalChildSubqueryColumns']) ? $options['additionalChildSubqueryColumns'] : '';
        $childJoinCondition = isset($options['childJoinCondition']) ? $options['childJoinCondition'] : 'child.entity_id = l.linked_product_id';
        $additionalJoins = isset($options['additionalJoins']) ? $options['additionalJoins'] : array();
        $whereConditions = isset($options['whereConditions']) ? $options['whereConditions'] : array();
        $orderBy = isset($options['orderBy']) ? $options['orderBy'] : 'parent.sku';
        $storeIdsPlaceholder = $options['storeIdsPlaceholder'];
        $nPerPage = $options['nPerPage'];
        $tables = $options['tables'];


        // Build child columns for SELECT
        $childColumnsSql = '';
        foreach ($childColumns as $alias => $expression) {
            $childColumnsSql .= ",\n                    {$expression} AS {$alias}";
        }

        // Build additional JOINs
        $additionalJoinsSql = '';
        if (!empty($additionalJoins)) {
            $additionalJoinsSql = "\n" . implode("\n", $additionalJoins);
        }

        // Build additional WHERE conditions
        $additionalWhereSql = '';
        if (!empty($whereConditions)) {
            foreach ($whereConditions as $condition) {
                $additionalWhereSql .= "\n                    AND {$condition}";
            }
        }

        #$sql = "\n            SELECT DISTINCT\n                parent.entity_id AS Parent_ID,\n                parent.sku AS Parent_SKU,\n                pname.value AS Parent_Name,\n                parent_url_key.value AS Parent_URLKey,\n                parent.updated_at AS Parent_UpdatedAt,\n                pstatus.value AS Parent_Status,\n                child.entity_id AS Child_ID,\n                child.sku AS Child_SKU,\n                child_url_key.value AS Child_URLKey,\n                child_url_key.store_id AS Child_Store,\n                child_special_from.value AS Child_Special_From_Date,\n                child_special_to.value AS Child_Special_To_Date{$childColumnsSql}\n            FROM {$tables['productLink']} l\n            JOIN {$matchScoreExpr} ON {$childJoinCondition}\n            JOIN {$tables['product']} parent\n                ON parent.entity_id = l.product_id\n                AND parent.type_id = 'grouped'\n            JOIN {$tables['int']} pstatus\n                ON pstatus.entity_id = parent.entity_id\n                AND pstatus.attribute_id = 89\n                AND pstatus.store_id = 0\n                AND pstatus.value = 1\n            JOIN {$tables['varchar']} parent_url_key\n                ON parent_url_key.entity_id = parent.entity_id\n                AND parent_url_key.attribute_id = 90\n                AND parent_url_key.store_id = 0\n            JOIN {$tables['varchar']} pname\n                ON pname.entity_id = parent.entity_id\n                AND pname.attribute_id = 65\n                AND pname.store_id = 0\n            JOIN {$tables['varchar']} child_url_key\n                ON child_url_key.entity_id = child.entity_id\n                AND child_url_key.attribute_id = 90\n                AND child_url_key.store_id IN ({$storeIdsPlaceholder})\n            LEFT JOIN {$tables['datetime']} child_special_from\n                ON child_special_from.entity_id = child.entity_id\n                AND child_special_from.store_id = child_url_key.store_id\n                AND child_special_from.attribute_id = 71\n            LEFT JOIN {$tables['datetime']} child_special_to\n                ON child_special_to.entity_id = child.entity_id\n                AND child_special_to.store_id = child_url_key.store_id\n                AND child_special_to.attribute_id = 72{$additionalJoinsSql}\n            WHERE l.link_type_id = 3\n                AND parent.sku REGEXP '[0-9]{8}'{$additionalWhereSql}\n            ORDER BY {$orderBy}\n            LIMIT {$nPerPage}\n        ";

        $sql = "SELECT DISTINCT
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
                child_special_from.value AS Child_Special_From_Date,
                child_special_to.value AS Child_Special_To_Date{$childColumnsSql} 
                FROM {$tables['productLink']} l
                    JOIN (
                    SELECT c.* {$matchScoreExpr}
                    FROM (
                        SELECT child.entity_id, child.sku, cname.value AS name, cname.store_id AS store_id{$additionalChildSubqueryColumns}
                        FROM {$tables['product']} child
                        JOIN {$tables['varchar']} cname
                            ON cname.entity_id = child.entity_id
                            AND cname.attribute_id = 65
                            AND cname.store_id IN ({$storeIdsPlaceholder})
                    ) c
                ) child ON {$childJoinCondition}
                    JOIN {$tables['product']} parent
                        ON parent.entity_id = l.product_id
                        AND parent.type_id = 'grouped'
                    JOIN {$tables['int']} pstatus
                        ON pstatus.entity_id = parent.entity_id
                        AND pstatus.attribute_id = 89
                        AND pstatus.store_id = 0
                        AND pstatus.value = 1
                    JOIN {$tables['varchar']} parent_url_key
                        ON parent_url_key.entity_id = parent.entity_id
                        AND parent_url_key.attribute_id = 90
                        AND parent_url_key.store_id = 0
                    JOIN {$tables['varchar']} pname
                        ON pname.entity_id = parent.entity_id
                        AND pname.attribute_id = 65
                        AND pname.store_id = 0
                    JOIN {$tables['varchar']} child_url_key
                        ON child_url_key.entity_id = child.entity_id
                        AND child_url_key.attribute_id = 90
                        AND child_url_key.store_id = child.store_id
                    JOIN {$tables['varchar']} cname
                        ON cname.entity_id = child.entity_id
                        AND cname.store_id = child.store_id
                        AND cname.attribute_id = 65
                    JOIN {$tables['datetime']} child_special_from
                        ON child_special_from.entity_id = child.entity_id
                        AND child_special_from.store_id = child.store_id
                        AND child_special_from.attribute_id = 71
                        AND child_special_from.value < NOW()
                    JOIN {$tables['datetime']} child_special_to
                        ON child_special_to.entity_id = child.entity_id
                        AND child_special_to.store_id = child.store_id
                        AND child_special_to.attribute_id = 72
                        AND child_special_to.value > NOW(){$additionalJoinsSql}
                    WHERE l.link_type_id = 3
                        AND parent.sku REGEXP '[0-9]{8}'{$additionalWhereSql}
                        ORDER BY {$orderBy}
                        LIMIT {$nPerPage}";
        return $sql;
    }

    public function buildSKUSearchQuery() {
        $productTable = "catalog_product_entity";
        $productLinkTable = "catalog_product_link";
        $varcharTable = 'catalog_product_entity_varchar';
        $intTable = 'catalog_product_entity_int';
        $datetimeTable = 'catalog_product_entity_datetime';
        $storeIdsPlaceholder = '6,8,9';
        
        $nPerPage = 100;

        
        $childSubquery = "(SELECT
                *,
                LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,
                LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body
            FROM {$productTable}) child";

        $sql = $this->_buildDealSearchQuery(array(
            'childSource' => $childSubquery,
            'whereConditions' => array(
                "child.prefix = 'k5'",
                "child.body LIKE CONCAT('%','AFA6750-M','%')"
            ),
            'orderBy' => 'parent.sku',
            'storeIdsPlaceholder' => $storeIdsPlaceholder,
            'nPerPage' => $nPerPage,
            'tables' => array(
                'product' => $productTable,
                'productLink' => $productLinkTable,
                'varchar' => $varcharTable,
                'int' => $intTable,
                'datetime' => $datetimeTable
            )
        ));
        print "$sql";
        return $sql;
        }

    public function buildSKUTrigramQueryTrigram() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $storeIdsPlaceholder = '6,8,9';
            
            $prefix = 'k5';
            $body = 'AFA6750-M';
            $nPerPage = 100;
            $bodyLen = strlen($body);
            if ($bodyLen >= 3) {
                $ngramSize = 3;
            } elseif ($bodyLen == 2) {
                $ngramSize = 2;
            } else {
                $ngramSize = 1;
            }

            // Generate n-grams (sliding windows) and build match_score SQL expression
            $matchScoreParts = array();

            if (empty($windows)) {
                $numWindows = $bodyLen - $ngramSize + 1;
            } else {
                $numWindows = min($bodyLen - $ngramSize + 1,4);
            }
            
            for ($i = 0; $i < $numWindows; $i++) {
                $ngram = substr($body, $i, $ngramSize);
                $matchScoreParts[] = "\n\t\t\t(LENGTH(c.body) - LENGTH(REPLACE(c.body, '{$ngram}', ''))) / {$ngramSize}";
            }
            $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';

            // Build child subquery with SKU parsing and n-gram scoring
            $childSubquery = "(
                    SELECT
                        *,
                        ({$matchScoreExpr}) AS match_score
                    FROM (
                        SELECT
                            *,
                            LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,
                            LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body
                        FROM {$productTable}
                    ) c
                ) child";

            $sql = $this->_buildDealSearchQuery(array(
                'childColumns' => array(
                    'Child_Match_Score' => 'child.match_score'
                ),
                'additionalChildSubqueryColumns' => ",LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body",
                'matchScoreExpr' => ", ({$matchScoreExpr}) AS match_score",
                'whereConditions' => array(
                    "child.prefix = '{$prefix}'",
                    'child.match_score > 0'
                ),
                'orderBy' => 'child.match_score DESC',
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'nPerPage' => $nPerPage,
                'tables' => array(
                        'product' => $productTable,
                        'productLink' => $productLinkTable,
                        'varchar' => $varcharTable,
                        'int' => $intTable,
                        'datetime' => $datetimeTable
                        )
            ));
            print "$sql";
    }

    public function buildDescQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $textTable = 'catalog_product_entity_text';
            $storeIdsPlaceholder = '6,8,9';
            
            $prefix = 'k1';
            $body = 'oliva';
            $nPerPage = 100;
            $bodyLen = strlen($body);
            if ($bodyLen >= 3) {
                $ngramSize = 3;
            } elseif ($bodyLen == 2) {
                $ngramSize = 2;
            } else {
                $ngramSize = 1;
            }

            // Generate n-grams (sliding windows) and build match_score SQL expression
            $matchScoreParts = array();

            if (empty($windows)) {
                $numWindows = $bodyLen - $ngramSize + 1;
            } else {
                $numWindows = min($bodyLen - $ngramSize + 1,4);
            }
            
            for ($i = 0; $i < $numWindows; $i++) {
                $ngram = substr($body, $i, $ngramSize);
                $matchScoreParts[] = "\n\t\t\t(LENGTH(c.body) - LENGTH(REPLACE(c.body, '{$ngram}', ''))) / {$ngramSize}";
            }
            $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';

            // Build child subquery with SKU parsing and n-gram scoring
            $childSubquery = "(
                    SELECT
                        *,
                        ({$matchScoreExpr}) AS match_score
                    FROM (
                        SELECT
                            *,
                            LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,
                            LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body
                        FROM {$productTable}
                    ) c
                ) child";

            $sql = $this->_buildDealSearchQuery(array(
                'childColumns' => array(
                    'Child_Description' => 'descr.value'
                ),
                'childSource' => "{$productTable} child",
                'additionalJoins' => array(
                    "                JOIN {$textTable} descr",
                    "                    ON descr.entity_id = child.entity_id",
                    "                    AND descr.attribute_id = 66",
                    "                    AND descr.store_id = child_url_key.store_id"
                ),
                'whereConditions' => array(
                    "LOWER(descr.value) LIKE CONCAT('%', LOWER('oliva'), '%')"
                ),
                'orderBy' => 'parent.sku',
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'nPerPage' => $nPerPage,
                'tables' => array(
                    'product' => $productTable,
                    'productLink' => $productLinkTable,
                    'varchar' => $varcharTable,
                    'int' => $intTable,
                    'datetime' => $datetimeTable,
                    'text' => $textTable
                )
            ));
            print "$sql";
    }

    public function buildNameQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $textTable = 'catalog_product_entity_text';
            $storeIdsPlaceholder = '6,8,9';
            
            $prefix = 'k1';
            $body = 'oliva';
            $nPerPage = 100;
            $bodyLen = strlen($body);
            if ($bodyLen >= 3) {
                $ngramSize = 3;
            } elseif ($bodyLen == 2) {
                $ngramSize = 2;
            } else {
                $ngramSize = 1;
            }

            // Generate n-grams (sliding windows) and build match_score SQL expression
            $matchScoreParts = array();

            if (empty($windows)) {
                $numWindows = $bodyLen - $ngramSize + 1;
            } else {
                $numWindows = min($bodyLen - $ngramSize + 1,4);
            }
            
            for ($i = 0; $i < $numWindows; $i++) {
                $ngram = substr($body, $i, $ngramSize);
                $matchScoreParts[] = "\n\t\t\t(LENGTH(c.body) - LENGTH(REPLACE(c.body, '{$ngram}', ''))) / {$ngramSize}";
            }
            $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';

            // Build child subquery with SKU parsing and n-gram scoring
            $childSubquery = "(
                    SELECT
                        *,
                        ({$matchScoreExpr}) AS match_score
                    FROM (
                        SELECT
                            *,
                            LOWER(SUBSTRING_INDEX(sku, '-', 1)) AS prefix,
                            LOWER(SUBSTRING(sku, LOCATE('-', sku) + 1, LENGTH(sku))) AS body
                        FROM {$productTable}
                    ) c
                ) child";

            $sql = $this->_buildDealSearchQuery(array(
                'childColumns' => array(
                    'Child_Name' => 'cname.value'
                ),
                'childSource' => "{$productTable} child",
                'whereConditions' => array(
                    "LOWER(cname.value) LIKE CONCAT('%', LOWER('oliva'), '%')"
                ),
                'orderBy' => 'parent.sku',
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'nPerPage' => $nPerPage,
                'tables' => array(
                    'product' => $productTable,
                    'productLink' => $productLinkTable,
                    'varchar' => $varcharTable,
                    'int' => $intTable,
                    'datetime' => $datetimeTable
                )
            ));
            print "$sql";
    }

    public function buildNameTrigramQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $textTable = 'catalog_product_entity_text';
            $storeIdsPlaceholder = '6,8,9';
            $nPerPage = 100;
            
            $query = 'Arturo Fuente Double Chateau MAD 5pk';
            $queryLen = strlen($query);
            if ($queryLen >= 3) {
                $ngramSize = 3;
            } elseif ($queryLen == 2) {
                $ngramSize = 2;
            } else {
                $ngramSize = 1;
            }

            // Generate n-grams (sliding windows) and build match_score SQL expression
            $matchScoreParts = array();
            if (empty($windows)) {
                $numWindows = $queryLen - $ngramSize + 1;
            } else {
                $numWindows = min($queryLen - $ngramSize + 1,4);
            }

            for ($i = 0; $i < $numWindows; $i++) {
                $ngram = substr($query, $i, $ngramSize);
                $matchScoreParts[] = "(LENGTH(LOWER(c.name)) - LENGTH(REPLACE(LOWER(c.name), '{$ngram}', ''))) / {$ngramSize}";
            }
            $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';

            $sql = $this->_buildDealSearchQuery(array(
                'childColumns' => array(
                    'Child_Match_Score' => 'child.match_score'
                ),
                'matchScoreExpr' => ", ({$matchScoreExpr}) AS match_score",
                'whereConditions' => array(
                    'child.match_score > 0'
                ),
                'orderBy' => 'child.match_score DESC',
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'nPerPage' => $nPerPage,
                'tables' => array(
                    'product' => $productTable,
                    'productLink' => $productLinkTable,
                    'varchar' => $varcharTable,
                    'int' => $intTable,
                    'datetime' => $datetimeTable
                )
            ));
            print "$sql";
    }

   }

    

$tds = new TestDealSearch();
$tds->buildSKUTrigramQueryTrigram();

?>