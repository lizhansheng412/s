-- =============================================================================
-- PostgreSQL性能优化配置 V2
-- =============================================================================
-- 硬件配置：32GB内存 + 16核CPU
-- 数据规模：2TB，2000万+记录/表
-- =============================================================================

-- =============================================================================
-- 1. 表存储参数优化
-- =============================================================================

DO $$
DECLARE
    table_names TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    i INTEGER;
    partition_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        RAISE NOTICE '优化表 %...', table_name;
        
        -- 为每个分区设置优化参数
        FOR i IN 0..63 LOOP
            partition_name := table_name || '_p' || i;
            
            -- 设置填充因子（留10%空间给UPDATE）
            EXECUTE format('ALTER TABLE %I SET (fillfactor = 90)', partition_name);
            
            -- 设置autovacuum参数
            EXECUTE format(
                'ALTER TABLE %I SET (
                    autovacuum_vacuum_scale_factor = 0.05,
                    autovacuum_analyze_scale_factor = 0.02,
                    autovacuum_vacuum_cost_delay = 10,
                    autovacuum_vacuum_cost_limit = 1000
                )',
                partition_name
            );
        END LOOP;
        
        RAISE NOTICE '✓ 表 % 优化完成（64个分区）', table_name;
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '✓ 所有表存储参数优化完成！';
END $$;

-- =============================================================================
-- 2. JSONB字段存储策略优化
-- =============================================================================

DO $$
DECLARE
    table_names TEXT[] := ARRAY[
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    i INTEGER;
    partition_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        FOR i IN 0..63 LOOP
            partition_name := table_name || '_p' || i;
            
            -- 大字段使用EXTENDED策略（压缩+TOAST）
            IF table_name IN ('s2orc', 's2orc_v2', 'papers', 'citations') THEN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN data SET STORAGE EXTENDED', partition_name);
            -- 小字段使用MAIN策略（尽量内联）
            ELSE
                EXECUTE format('ALTER TABLE %I ALTER COLUMN data SET STORAGE MAIN', partition_name);
            END IF;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE '✓ JSONB存储策略优化完成！';
END $$;

-- =============================================================================
-- 3. 统计信息收集优化
-- =============================================================================

DO $$
DECLARE
    table_names TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    i INTEGER;
    partition_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        FOR i IN 0..63 LOOP
            partition_name := table_name || '_p' || i;
            
            -- 增加统计信息采样
            EXECUTE format('ALTER TABLE %I ALTER COLUMN corpusid SET STATISTICS 1000', partition_name);
            
            -- 字段表的data列也增加统计
            IF table_name != 'corpus_registry' THEN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN data SET STATISTICS 500', partition_name);
            END IF;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE '✓ 统计信息参数优化完成！';
END $$;

-- =============================================================================
-- 4. 维护函数
-- =============================================================================

-- 函数：优化单个表
CREATE OR REPLACE FUNCTION optimize_table(p_table_name TEXT)
RETURNS TEXT AS $$
DECLARE
    start_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := clock_timestamp();
    
    -- 执行VACUUM ANALYZE
    EXECUTE format('VACUUM ANALYZE %I', p_table_name);
    
    duration := clock_timestamp() - start_time;
    
    RETURN format('表 %s 优化完成，耗时: %s', p_table_name, duration);
END;
$$ LANGUAGE plpgsql;

-- 函数：优化所有表
CREATE OR REPLACE FUNCTION optimize_all_tables()
RETURNS TEXT AS $$
DECLARE
    table_names TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    start_time TIMESTAMP;
    total_duration INTERVAL;
    count INTEGER := 0;
BEGIN
    start_time := clock_timestamp();
    
    FOREACH table_name IN ARRAY table_names
    LOOP
        EXECUTE format('VACUUM ANALYZE %I', table_name);
        count := count + 1;
        
        RAISE NOTICE '✓ 已优化: % (%/%)', table_name, count, array_length(table_names, 1);
    END LOOP;
    
    total_duration := clock_timestamp() - start_time;
    
    RETURN format('已优化 %s 个表，总耗时: %s', count, total_duration);
END;
$$ LANGUAGE plpgsql;

-- 函数：获取表大小统计
CREATE OR REPLACE FUNCTION get_table_sizes()
RETURNS TABLE (
    table_name TEXT,
    total_size TEXT,
    table_size TEXT,
    indexes_size TEXT,
    row_count BIGINT
) AS $$
DECLARE
    tables TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    tbl TEXT;
    rec RECORD;
BEGIN
    FOREACH tbl IN ARRAY tables
    LOOP
        EXECUTE format(
            'SELECT 
                %L::TEXT as table_name,
                pg_size_pretty(pg_total_relation_size(%L)) as total_size,
                pg_size_pretty(pg_relation_size(%L)) as table_size,
                pg_size_pretty(pg_indexes_size(%L)) as indexes_size,
                (SELECT COUNT(*) FROM %I) as row_count',
            tbl, tbl, tbl, tbl, tbl
        ) INTO rec;
        
        table_name := rec.table_name;
        total_size := rec.total_size;
        table_size := rec.table_size;
        indexes_size := rec.indexes_size;
        row_count := rec.row_count;
        
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 函数：获取数据库整体统计
CREATE OR REPLACE FUNCTION get_database_stats()
RETURNS TABLE (
    metric TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'Database Size'::TEXT, 
           pg_size_pretty(pg_database_size(current_database()))::TEXT
    UNION ALL
    SELECT 'Total Tables'::TEXT, 
           COUNT(*)::TEXT 
    FROM pg_tables 
    WHERE schemaname = 'public'
    UNION ALL
    SELECT 'Total Indexes'::TEXT, 
           COUNT(*)::TEXT 
    FROM pg_indexes 
    WHERE schemaname = 'public'
    UNION ALL
    SELECT 'Active Connections'::TEXT, 
           COUNT(*)::TEXT 
    FROM pg_stat_activity 
    WHERE datname = current_database()
    UNION ALL
    SELECT 'Cache Hit Ratio'::TEXT,
           ROUND(
               SUM(blks_hit)::NUMERIC / NULLIF(SUM(blks_hit + blks_read), 0) * 100, 2
           )::TEXT || '%'
    FROM pg_stat_database
    WHERE datname = current_database();
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 5. 创建统计视图
-- =============================================================================

-- 视图：数据集统计
CREATE OR REPLACE VIEW dataset_statistics AS
SELECT 
    (SELECT COUNT(*) FROM corpus_registry) AS total_corpusids,
    (SELECT COUNT(*) FROM corpus_registry WHERE is_exist_papers = TRUE) AS papers_corpusids,
    (SELECT COUNT(*) FROM papers) AS papers_count,
    (SELECT COUNT(*) FROM abstracts) AS abstracts_count,
    (SELECT COUNT(*) FROM tldrs) AS tldrs_count,
    (SELECT COUNT(*) FROM s2orc) AS s2orc_count,
    (SELECT COUNT(*) FROM s2orc_v2) AS s2orc_v2_count,
    (SELECT COUNT(*) FROM citations) AS citations_count,
    (SELECT COUNT(*) FROM authors) AS authors_count,
    (SELECT COUNT(*) FROM embeddings_specter_v1) AS embeddings_v1_count,
    (SELECT COUNT(*) FROM embeddings_specter_v2) AS embeddings_v2_count,
    (SELECT COUNT(*) FROM paper_ids) AS paper_ids_count,
    (SELECT COUNT(*) FROM publication_venues) AS venues_count;

COMMENT ON VIEW dataset_statistics IS '数据集统计视图';

-- =============================================================================
-- 6. 更新所有表的统计信息
-- =============================================================================

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
-- 优化配置完成
-- =============================================================================
-- 说明：
-- 1. 表存储参数已优化（fillfactor, autovacuum）
-- 2. JSONB存储策略已设置（EXTENDED/MAIN）
-- 3. 统计信息采样已增加
-- 4. 维护函数已创建
-- 5. 统计视图已创建
--
-- 建议在 postgresql.conf 中添加以下配置（需要重启）：
--
-- # 内存配置（32GB系统）
-- shared_buffers = 8GB
-- effective_cache_size = 24GB
-- maintenance_work_mem = 2GB
-- work_mem = 128MB
--
-- # WAL配置
-- wal_buffers = 32MB
-- max_wal_size = 8GB
-- min_wal_size = 2GB
-- checkpoint_timeout = 30min
-- checkpoint_completion_target = 0.9
--
-- # 并行配置（16核CPU）
-- max_parallel_workers_per_gather = 4
-- max_parallel_workers = 12
-- max_worker_processes = 16
--
-- # 查询优化
-- random_page_cost = 1.1
-- effective_io_concurrency = 200
-- jit = on
--
-- # 分区优化（关键！）
-- enable_partition_pruning = on
-- constraint_exclusion = partition
--
-- # 连接池
-- max_connections = 200
--
-- # Autovacuum
-- autovacuum = on
-- autovacuum_max_workers = 4
-- autovacuum_naptime = 30s
-- =============================================================================

