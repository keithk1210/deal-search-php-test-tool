<?php

class TestDealSearch {
    protected function _buildDealSearchQuery($options,$query)
    {
        $matchScoreExpr = isset($options['matchScoreExpr']) ? $options['matchScoreExpr'] : '';
        $childJoinCondition = isset($options['childJoinCondition']) ? $options['childJoinCondition'] : 'child.entity_id = l.linked_product_id';
        $additionalJoins = isset($options['additionalJoins']) ? $options['additionalJoins'] : array();
        $whereConditions = isset($options['whereConditions']) ? $options['whereConditions'] : array();
        $orderBy = isset($options['orderBy']) ? $options['orderBy'] : 'parent.sku';
        $storeIdsPlaceholder = $options['storeIdsPlaceholder'];
        $nPerPage = $options['nPerPage'];
        $tables = $options['tables'];
        $exactMatchColumn = $options['exact_match_col'];
        $matchingNGramsExpr = $options['matchingNGramsExpr'];
        
        $additionalJoinsSql = '';
        if (!empty($additionalJoins)) {
            $additionalJoinsSql = "\n" . implode("\n", $additionalJoins);
        }

        $only_active_deals = empty($query);

        $baseChildColumns = $only_active_deals ? "'' AS Child_ID,
                '' AS Child_SKU,
                '' AS Child_Name,
                -1 AS Child_Store,
                '' AS Child_URLKey,
                '' AS Child_Special_From_Date,
                '' AS Child_Special_To_Date,
                '' AS Child_Match_Score, " : "child.entity_id AS Child_ID,
                child.sku AS Child_SKU,
                cname.value AS Child_Name,
                child.store_id AS Child_Store,
                child_url_key.value AS Child_URLKey,
                child_special_from.value AS Child_Special_From_Date,
                child_special_to.value AS Child_Special_To_Date,
                child.match_score AS Child_Match_Score,
                {$exactMatchColumn},
                {$matchingNGramsExpr}";

        $childSubq = $only_active_deals ? '' : "JOIN (
                    SELECT c.* {$matchScoreExpr}
                    FROM (
                        SELECT child.entity_id, child.sku, cname.value AS name, cname.store_id AS store_id
                        FROM {$tables['product']} child
                        JOIN {$tables['varchar']} cname
                            ON cname.entity_id = child.entity_id
                            AND cname.attribute_id = 65
                            AND cname.store_id IN ({$storeIdsPlaceholder})
                    ) c
                ) child ON {$childJoinCondition}";

        $childJoins = $only_active_deals ? '' : "JOIN {$tables['varchar']} child_url_key
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
                        AND child_special_from.value <= CONCAT(DATE(NOW()),' 00:00:00') 
                    JOIN {$tables['datetime']} child_special_to
                        ON child_special_to.entity_id = child.entity_id
                        AND child_special_to.store_id = child.store_id
                        AND child_special_to.attribute_id = 72
                        AND child_special_to.value >= CONCAT(DATE(NOW()),' 00:00:00')";

        $minNTrigrams = strlen($query) > 3 ? 2 : 1;

        $additionalWhereSql = $only_active_deals ? '' : " AND child.match_score >= {$minNTrigrams}";

        $orderBy = $only_active_deals ? 'parent.sku' : 'Exact_Match DESC, child.match_score DESC';

        $sql = "SELECT DISTINCT
                parent.entity_id AS Parent_ID,
                parent.sku AS Parent_SKU,
                pname.value AS Parent_Name,
                pname_alt.value AS Parent_Alternate_Name,
                parent_url_key.value AS Parent_URLKey,
                parent_url_path.store_id AS Parent_Store,
                parent.updated_at AS Parent_UpdatedAt,
                pstatus.value AS Parent_Status,
                {$baseChildColumns}
                deal.deal_id AS Deal_ID,
                deal.begin_at AS Deal_Begin_At,
                deal.end_at AS Deal_End_At,
                deal.deal_type AS Deal_Type
                FROM {$tables['productLink']} l
                    {$childSubq}
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
                    JOIN {$tables['varchar']} parent_url_path
                        ON parent_url_path.entity_id = parent.entity_id
                        AND parent_url_path.attribute_id = 91
                        AND parent_url_path.store_id in ({$storeIdsPlaceholder})
                    JOIN {$tables['varchar']} pname
                        ON pname.entity_id = parent.entity_id
                        AND pname.attribute_id = 65
                        AND pname.store_id = 0
                    JOIN {$tables['varchar']} pname_alt
                        ON pname_alt.entity_id = parent.entity_id
                        AND pname_alt.attribute_id = 174
                        AND pname_alt.store_id = 0
                    {$childJoins}
                    JOIN {$tables['categoryProduct']} cat_prod
                        ON cat_prod.product_id = parent.entity_id
                        AND cat_prod.category_id > 2
                    JOIN {$tables['deal']} deal
                        ON deal.category_id = cat_prod.category_id
                        AND deal.begin_at <= NOW()
                        AND deal.end_at >= NOW(){$additionalJoinsSql}
                    WHERE l.link_type_id = 3
                        AND parent.sku REGEXP '[0-9]{8}'{$additionalWhereSql}
                        ORDER BY {$orderBy}
                        LIMIT {$nPerPage}";
        return $sql;
    }

    protected function _buildNGramExpressions($query,$field) {
            // Generate n-grams (sliding windows) and build match_score SQL expression
            $query_no_ws = str_replace(" ",'',$query);
            $queryLen = strlen($query_no_ws);
            if ($queryLen >= 3) {
                $ngramSize = 3;
            } elseif ($queryLen == 2) {
                $ngramSize = 2;
            } else {
                $ngramSize = 1;
            }
            $matchScoreParts = array();

            if (empty($windows)) {
                $numWindows = $queryLen - $ngramSize + 1;
            } else {
                $numWindows = min($queryLen - $ngramSize + 1,4);
            }
            for ($i = 0; $i < $numWindows; $i++) {
                $ngram = substr($query_no_ws, $i, $ngramSize);

                $subq_field = "REPLACE(LOWER(c.{$field}),' ','')";
                $q_field = "REPLACE(LOWER(child.{$field}),' ','')";

                $matchScoreParts[] = "\n\t\t\t(LENGTH($subq_field) - LENGTH(REPLACE($subq_field, '{$ngram}', ''))) / {$ngramSize}";

                $matchingNGrams[] = "\n\t\t\t\t\tIF(((LENGTH($q_field) - LENGTH(REPLACE($q_field, '{$ngram}', ''))) / {$ngramSize}) >= 1,'{$ngram}\n','')";
            }
            $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';
            $matchingNGramsExpr = !empty($matchingNGrams) ? "CONCAT(" . implode(',',$matchingNGrams) . ") as NGrams,": '';
            return [$matchScoreExpr,$matchingNGramsExpr];
    }

    // protected function _buildNGramExpressions($query,$field) {
    //         // Generate n-grams (sliding windows) and build match_score SQL expression
    //         $queryLen = strlen($query);
    //         if ($queryLen >= 3) {
    //             $ngramSize = 3;
    //         } elseif ($queryLen == 2) {
    //             $ngramSize = 2;
    //         } else {
    //             $ngramSize = 1;
    //         }
    //         $matchScoreParts = array();

    //         if (empty($windows)) {
    //             $numWindows = $queryLen - $ngramSize + 1;
    //         } else {
    //             $numWindows = min($queryLen - $ngramSize + 1,4);
    //         }
    //         for ($i = 0; $i < $numWindows; $i++) {

    //             $subq_field = "REGEXP_REPLACE(LOWER(c.{$field}),'\s','')";
    //             $main_q_field = "REGEXP_REPLACE(LOWER(child.{$field}),'\s','')";

    //             $ngram = substr($query, $i, $ngramSize);
    //             $matchScoreParts[] = "\n\t\t\t(LENGTH($subq_field) - LENGTH(REPLACE($subq_field), '{$ngram}', ''))) / {$ngramSize}";

    //             $matchingNGrams[] = "\n\t\t\t\t\tIF(((LENGTH($main_q_field) - LENGTH(REPLACE($main_q_field, '{$ngram}', ''))) / {$ngramSize}) >= 1,'{$ngram}\n','')";
    //         }
    //         $matchScoreExpr = !empty($matchScoreParts) ? implode(' + ', $matchScoreParts) : '0';
    //         $matchingNGramsExpr = !empty($matchingNGrams) ? "CONCAT(" . implode(',',$matchingNGrams) . ") as NGrams,": '';
    //         return [$matchScoreExpr,$matchingNGramsExpr];
    // }

    public function buildSKUQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $dealTable ='wiserobot_deal';
            $categoryProductTable = 'catalog_category_product';
            $storeIdsPlaceholder = '6,8,9';
            
            $query = 'kx-avx5050';
            $nPerPage = 100;

            list($matchScoreExpr,$matchingNGramsExpr) = $this->_buildNGramExpressions($query,'sku');
        
            $sql = $this->_buildDealSearchQuery(array(
                'matchScoreExpr' => ", ({$matchScoreExpr}) AS match_score",
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'matchingNGramsExpr' => $matchingNGramsExpr,
                'nPerPage' => $nPerPage,
                'exact_match_col' => 
                "CASE LOWER('{$query}') = LOWER(child.sku)
                    WHEN 1 THEN ' exact_match'
                    ELSE ''
                    END AS Exact_Match",
                'tables' => array(
                        'product' => $productTable,
                        'productLink' => $productLinkTable,
                        'varchar' => $varcharTable,
                        'int' => $intTable,
                        'datetime' => $datetimeTable,
                        'deal' => $dealTable,
                        'categoryProduct' => $categoryProductTable
                        )
            ),$query);
            print "$sql";
    }

    public function buildNameQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $textTable = 'catalog_product_entity_text';
            $dealTable ='wiserobot_deal';
            $categoryProductTable = 'catalog_category_product';
            $storeIdsPlaceholder = '6,8,9';
            $nPerPage = 100;
            
            $query = 'arturo fuente';
            [$matchScoreExpr,$matchingNGramsExpr] = $this->_buildNGramExpressions($query,'name');

            // Build child subquery with name and n-gram scoring
            $sql = $this->_buildDealSearchQuery(array(
                'matchScoreExpr' => ", ({$matchScoreExpr}) AS match_score",
                'matchingNGramsExpr' => $matchingNGramsExpr,
                'storeIdsPlaceholder' => $storeIdsPlaceholder,
                'nPerPage' => $nPerPage,
                'exact_match_col' => 
                "CASE LOWER('{$query}') = LOWER(child.name)
                    WHEN 1 THEN ' exact_match'
                    ELSE ''
                    END AS Exact_Match",
                'tables' => array(
                    'product' => $productTable,
                    'productLink' => $productLinkTable,
                    'varchar' => $varcharTable,
                    'int' => $intTable,
                    'datetime' => $datetimeTable,
                    'deal' => $dealTable,
                    'categoryProduct' => $categoryProductTable
                )
            ),$query);
            print "$sql";
    }

    public function buildActiveDealsQuery() {
            $productTable = "catalog_product_entity";
            $productLinkTable = "catalog_product_link";
            $varcharTable = 'catalog_product_entity_varchar';
            $intTable = 'catalog_product_entity_int';
            $datetimeTable = 'catalog_product_entity_datetime';
            $dealTable ='wiserobot_deal';
            $categoryProductTable = 'catalog_category_product';
            $storeIdsPlaceholder = '6,8,9';
            
            $query = '';
            $nPerPage = 100;
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
                $matchScoreParts[] = "\n\t\t\t(LENGTH(LOWER(c.sku)) - LENGTH(REPLACE(LOWER(c.sku), '{$ngram}', ''))) / {$ngramSize}";
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
                        'datetime' => $datetimeTable,
                        'deal' => $dealTable,
                        'categoryProduct' => $categoryProductTable
                        )
            ),$query);
            print "$sql";
    }
   }

$tds = new TestDealSearch();
$tds->buildNameQuery();

?>