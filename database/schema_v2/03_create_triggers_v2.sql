-- =============================================================================
-- S2ORC 语料库数据库触发器创建 V2
-- =============================================================================
-- 触发器功能：
-- 1. 自动更新 update_time 字段
-- 2. 提供禁用/启用触发器的函数（批量操作时使用）
-- =============================================================================

-- =============================================================================
-- 1. 创建触发器函数：自动更新 update_time
-- =============================================================================

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_timestamp() IS '触发器函数：自动更新update_time为当前时间';

-- =============================================================================
-- 2. 为所有表创建触发器
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
    trigger_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        trigger_name := 'trg_update_' || table_name;
        
        -- 创建UPDATE触发器
        EXECUTE format(
            'CREATE TRIGGER %I
             BEFORE UPDATE ON %I
             FOR EACH ROW
             EXECUTE FUNCTION update_timestamp()',
            trigger_name, table_name
        );
        
        RAISE NOTICE '✓ 触发器创建完成: %', trigger_name;
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '✓ 所有触发器创建完成！共12个触发器';
END $$;

-- =============================================================================
-- 3. 批量操作辅助函数：禁用/启用触发器
-- =============================================================================

-- 函数：禁用所有触发器（批量插入时使用）
CREATE OR REPLACE FUNCTION disable_all_triggers()
RETURNS TEXT AS $$
DECLARE
    table_names TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    count INTEGER := 0;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        EXECUTE format('ALTER TABLE %I DISABLE TRIGGER ALL', table_name);
        count := count + 1;
    END LOOP;
    
    RETURN format('已禁用 %s 个表的触发器', count);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION disable_all_triggers() IS '禁用所有表的触发器（批量插入时使用）';

-- 函数：启用所有触发器
CREATE OR REPLACE FUNCTION enable_all_triggers()
RETURNS TEXT AS $$
DECLARE
    table_names TEXT[] := ARRAY[
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    ];
    table_name TEXT;
    count INTEGER := 0;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE TRIGGER ALL', table_name);
        count := count + 1;
    END LOOP;
    
    RETURN format('已启用 %s 个表的触发器', count);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION enable_all_triggers() IS '启用所有表的触发器';

-- 函数：禁用单个表的触发器
CREATE OR REPLACE FUNCTION disable_table_triggers(p_table_name TEXT)
RETURNS TEXT AS $$
BEGIN
    EXECUTE format('ALTER TABLE %I DISABLE TRIGGER ALL', p_table_name);
    RETURN format('已禁用表 %s 的触发器', p_table_name);
END;
$$ LANGUAGE plpgsql;

-- 函数：启用单个表的触发器
CREATE OR REPLACE FUNCTION enable_table_triggers(p_table_name TEXT)
RETURNS TEXT AS $$
BEGIN
    EXECUTE format('ALTER TABLE %I ENABLE TRIGGER ALL', p_table_name);
    RETURN format('已启用表 %s 的触发器', p_table_name);
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 4. 查看触发器状态的函数
-- =============================================================================

CREATE OR REPLACE FUNCTION get_trigger_status()
RETURNS TABLE (
    table_name TEXT,
    trigger_name TEXT,
    trigger_enabled TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.tgrelid::regclass::TEXT,
        t.tgname::TEXT,
        CASE 
            WHEN t.tgenabled = 'O' THEN 'ENABLED'
            WHEN t.tgenabled = 'D' THEN 'DISABLED'
            WHEN t.tgenabled = 'R' THEN 'REPLICA'
            WHEN t.tgenabled = 'A' THEN 'ALWAYS'
            ELSE 'UNKNOWN'
        END
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    WHERE c.relname IN (
        'corpus_registry',
        'papers', 'abstracts', 'tldrs', 's2orc', 's2orc_v2', 'citations',
        'authors', 'embeddings_specter_v1', 'embeddings_specter_v2', 
        'paper_ids', 'publication_venues'
    )
    AND NOT t.tgisinternal
    ORDER BY c.relname, t.tgname;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_trigger_status() IS '查看所有触发器的启用状态';

-- =============================================================================
-- 触发器创建完成
-- =============================================================================
-- 创建的资源：
--   - 1个触发器函数：update_timestamp()
--   - 12个触发器：为每个表自动更新update_time
--   - 5个管理函数：禁用/启用触发器、查看状态
-- =============================================================================

