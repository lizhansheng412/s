-- =============================================================================
-- S2ORC 语料库数据库表结构 V2 - 分表方案
-- =============================================================================
-- 设计说明：
-- 1. 主表 corpus_registry：记录所有corpusid及其在papers表中的存在状态
-- 2. 11个字段表：每个数据集一个独立的表
-- 3. 每个表都分64个哈希分区（针对2000万+记录优化）
-- 4. 所有表都有 insert_time 和 update_time 字段
-- =============================================================================

-- 创建必要的扩展
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================================
-- 1. 主表：corpus_registry（语料库注册表）
-- =============================================================================

DROP TABLE IF EXISTS corpus_registry CASCADE;

CREATE TABLE corpus_registry (
    corpusid BIGINT NOT NULL,
    is_exist_papers BOOLEAN DEFAULT FALSE,  -- 标记是否在papers表中
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE corpus_registry IS '语料库注册表：记录所有corpusid';
COMMENT ON COLUMN corpus_registry.is_exist_papers IS '标记该corpusid是否在papers表中存在';

-- 创建64个分区
DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE corpus_registry_p%s PARTITION OF corpus_registry 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ corpus_registry 创建完成：64个分区';
END $$;

-- =============================================================================
-- 2. 字段表：papers（论文基础数据 - 最重要的表）
-- =============================================================================

DROP TABLE IF EXISTS papers CASCADE;

CREATE TABLE papers (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE papers IS '论文基础数据表';

-- 创建64个分区
DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE papers_p%s PARTITION OF papers 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ papers 创建完成：64个分区';
END $$;

-- =============================================================================
-- 3. 字段表：abstracts（摘要）
-- =============================================================================

DROP TABLE IF EXISTS abstracts CASCADE;

CREATE TABLE abstracts (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE abstracts IS '论文摘要数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE abstracts_p%s PARTITION OF abstracts 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ abstracts 创建完成：64个分区';
END $$;

-- =============================================================================
-- 4. 字段表：tldrs（TLDR摘要）
-- =============================================================================

DROP TABLE IF EXISTS tldrs CASCADE;

CREATE TABLE tldrs (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE tldrs IS 'TLDR摘要数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE tldrs_p%s PARTITION OF tldrs 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ tldrs 创建完成：64个分区';
END $$;

-- =============================================================================
-- 5. 字段表：s2orc（S2ORC完整数据）
-- =============================================================================

DROP TABLE IF EXISTS s2orc CASCADE;

CREATE TABLE s2orc (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE s2orc IS 'S2ORC完整数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE s2orc_p%s PARTITION OF s2orc 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ s2orc 创建完成：64个分区';
END $$;

-- =============================================================================
-- 6. 字段表：s2orc_v2（S2ORC v2数据）
-- =============================================================================

DROP TABLE IF EXISTS s2orc_v2 CASCADE;

CREATE TABLE s2orc_v2 (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE s2orc_v2 IS 'S2ORC v2数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE s2orc_v2_p%s PARTITION OF s2orc_v2 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ s2orc_v2 创建完成：64个分区';
END $$;

-- =============================================================================
-- 7. 字段表：citations（引用数据）
-- =============================================================================

DROP TABLE IF EXISTS citations CASCADE;

CREATE TABLE citations (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE citations IS '论文引用数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE citations_p%s PARTITION OF citations 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ citations 创建完成：64个分区';
END $$;

-- =============================================================================
-- 8. 字段表：authors（作者数据）
-- =============================================================================

DROP TABLE IF EXISTS authors CASCADE;

CREATE TABLE authors (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE authors IS '论文作者数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE authors_p%s PARTITION OF authors 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ authors 创建完成：64个分区';
END $$;

-- =============================================================================
-- 9. 字段表：embeddings_specter_v1（嵌入向量v1）
-- =============================================================================

DROP TABLE IF EXISTS embeddings_specter_v1 CASCADE;

CREATE TABLE embeddings_specter_v1 (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE embeddings_specter_v1 IS '论文嵌入向量v1数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE embeddings_specter_v1_p%s PARTITION OF embeddings_specter_v1 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ embeddings_specter_v1 创建完成：64个分区';
END $$;

-- =============================================================================
-- 10. 字段表：embeddings_specter_v2（嵌入向量v2）
-- =============================================================================

DROP TABLE IF EXISTS embeddings_specter_v2 CASCADE;

CREATE TABLE embeddings_specter_v2 (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE embeddings_specter_v2 IS '论文嵌入向量v2数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE embeddings_specter_v2_p%s PARTITION OF embeddings_specter_v2 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ embeddings_specter_v2 创建完成：64个分区';
END $$;

-- =============================================================================
-- 11. 字段表：paper_ids（论文ID映射）
-- =============================================================================

DROP TABLE IF EXISTS paper_ids CASCADE;

CREATE TABLE paper_ids (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE paper_ids IS '论文ID映射数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE paper_ids_p%s PARTITION OF paper_ids 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ paper_ids 创建完成：64个分区';
END $$;

-- =============================================================================
-- 12. 字段表：publication_venues（发表场所）
-- =============================================================================

DROP TABLE IF EXISTS publication_venues CASCADE;

CREATE TABLE publication_venues (
    corpusid BIGINT NOT NULL,
    data JSONB NOT NULL,
    insert_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corpusid)
) PARTITION BY HASH (corpusid);

COMMENT ON TABLE publication_venues IS '论文发表场所数据表';

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE publication_venues_p%s PARTITION OF publication_venues 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
    RAISE NOTICE '✓ publication_venues 创建完成：64个分区';
END $$;

-- =============================================================================
-- 辅助函数
-- =============================================================================

-- 函数：获取corpusid对应的分区ID
CREATE OR REPLACE FUNCTION get_partition_id(p_corpusid BIGINT)
RETURNS INTEGER AS $$
BEGIN
    RETURN p_corpusid % 64;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 函数：获取corpusid对应的分区名
CREATE OR REPLACE FUNCTION get_partition_name(p_table_name TEXT, p_corpusid BIGINT)
RETURNS TEXT AS $$
BEGIN
    RETURN p_table_name || '_p' || (p_corpusid % 64);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 函数：统计某个表的总记录数
CREATE OR REPLACE FUNCTION count_table_records(p_table_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    record_count BIGINT;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO record_count;
    RETURN record_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 创建完成
-- =============================================================================
-- 总计：
--   - 12个主表（1个主表 + 11个字段表）
--   - 768个分区表（12 × 64）
-- =============================================================================

