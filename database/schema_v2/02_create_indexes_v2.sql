-- =============================================================================
-- S2ORC 语料库数据库索引创建 V2
-- =============================================================================
-- 索引策略：
-- 1. 主键索引（自动创建）：B-tree，用于快速单点查询
-- 2. GIN索引：用于JSONB字段的查询
-- 3. BRIN索引：用于时间字段的范围查询（占用空间小）
-- 4. 部分索引：只索引有实际数据的记录
-- =============================================================================

-- =============================================================================
-- 1. 主表 corpus_registry 的索引
-- =============================================================================

-- 主键索引已自动创建：corpus_registry_pkey on (corpusid)

-- 部分索引：只索引存在于papers表的记录
CREATE INDEX idx_registry_papers ON corpus_registry (corpusid) 
WHERE is_exist_papers = TRUE;

-- BRIN索引：时间范围查询
CREATE INDEX idx_registry_insert_time ON corpus_registry 
USING BRIN (insert_time);

CREATE INDEX idx_registry_update_time ON corpus_registry 
USING BRIN (update_time);

-- =============================================================================
-- 2. 为所有11个字段表批量创建索引
-- =============================================================================

DO $$
DECLARE
    table_names TEXT[] := ARRAY[
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    idx_name TEXT;
    start_time TIMESTAMP;
    elapsed INTERVAL;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        start_time := clock_timestamp();
        RAISE NOTICE '为表 % 创建索引...', table_name;
        
        -- 1. 主键索引已自动创建（不需要手动创建）
        
        -- 2. GIN索引：用于JSONB字段查询
        idx_name := 'idx_gin_' || table_name || '_data';
        EXECUTE format(
            'CREATE INDEX %I ON %I USING GIN (data)',
            idx_name, table_name
        );
        RAISE NOTICE '  ✓ GIN索引创建完成: %', idx_name;
        
        -- 3. BRIN索引：insert_time（时间范围查询，占用空间小）
        idx_name := 'idx_brin_' || table_name || '_insert';
        EXECUTE format(
            'CREATE INDEX %I ON %I USING BRIN (insert_time)',
            idx_name, table_name
        );
        RAISE NOTICE '  ✓ BRIN索引创建完成: %', idx_name;
        
        -- 4. BRIN索引：update_time
        idx_name := 'idx_brin_' || table_name || '_update';
        EXECUTE format(
            'CREATE INDEX %I ON %I USING BRIN (update_time)',
            idx_name, table_name
        );
        RAISE NOTICE '  ✓ BRIN索引创建完成: %', idx_name;
        
        elapsed := clock_timestamp() - start_time;
        RAISE NOTICE '✓ 表 % 索引创建完成（耗时 %）', table_name, elapsed;
        RAISE NOTICE '';
    END LOOP;
    
    RAISE NOTICE '✓ 所有索引创建完成！';
END $$;

-- =============================================================================
-- 辅助函数：索引管理
-- =============================================================================

-- 函数：获取索引大小统计
CREATE OR REPLACE FUNCTION get_index_sizes()
RETURNS TABLE (
    table_name TEXT,
    index_name TEXT,
    index_size TEXT,
    index_type TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        i.tablename::TEXT,
        i.indexname::TEXT,
        pg_size_pretty(pg_relation_size(i.schemaname||'.'||i.indexname))::TEXT,
        (SELECT indexdef FROM pg_indexes WHERE indexname = i.indexname)::TEXT
    FROM pg_indexes i
    WHERE i.schemaname = 'public'
    AND (
        i.tablename = 'corpus_registry' OR
        i.tablename IN (
            'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
            'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
            'paper_ids', 'publication_venues'
        )
    )
    ORDER BY pg_relation_size(i.schemaname||'.'||i.indexname) DESC;
END;
$$ LANGUAGE plpgsql;

-- 函数：分析索引使用情况
CREATE OR REPLACE FUNCTION analyze_index_usage()
RETURNS TABLE (
    table_name TEXT,
    index_name TEXT,
    index_scans BIGINT,
    tuples_read BIGINT,
    tuples_fetched BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        i.relname::TEXT,
        i.indexrelname::TEXT,
        i.idx_scan,
        i.idx_tup_read,
        i.idx_tup_fetch
    FROM pg_stat_user_indexes i
    WHERE i.schemaname = 'public'
    AND (
        i.relname = 'corpus_registry' OR
        i.relname IN (
            'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
            'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
            'paper_ids', 'publication_venues'
        )
    )
    ORDER BY i.idx_scan DESC;
END;
$$ LANGUAGE plpgsql;

-- 函数：重建所有索引
CREATE OR REPLACE FUNCTION rebuild_all_indexes()
RETURNS TEXT AS $$
DECLARE
    idx_name TEXT;
    result_msg TEXT := '';
    count INTEGER := 0;
BEGIN
    FOR idx_name IN 
        SELECT indexname 
        FROM pg_indexes 
        WHERE schemaname = 'public'
        AND (
            tablename = 'corpus_registry' OR
            tablename IN (
                'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
                'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
                'paper_ids', 'publication_venues'
            )
            OR tablename LIKE '%_p%'  -- 分区表
        )
    LOOP
        EXECUTE 'REINDEX INDEX CONCURRENTLY ' || idx_name;
        count := count + 1;
        
        IF count % 10 = 0 THEN
            RAISE NOTICE '已重建 % 个索引...', count;
        END IF;
    END LOOP;
    
    result_msg := format('已重建 %s 个索引', count);
    RETURN result_msg;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 更新统计信息
-- =============================================================================

-- 分析所有表
ANALYZE corpus_registry;
ANALYZE papers;
ANALYZE abstracts;
ANALYZE tldrs;
ANALYZE s2orc;
ANALYZE s2orc_v2;
ANALYZE citations;
ANALYZE authors;
ANALYZE embeddings_specter_v1;
ANALYZE embeddings_specter_v2;
ANALYZE paper_ids;
ANALYZE publication_venues;

-- =============================================================================
-- 索引创建完成
-- =============================================================================
-- 总计：
--   - 主键索引：12个表 × 1 = 12个（自动创建）
--   - GIN索引：11个字段表 × 1 = 11个
--   - BRIN索引：12个表 × 2 = 24个（insert_time + update_time）
--   - 部分索引：1个（corpus_registry.is_exist_papers）
-- =============================================================================

