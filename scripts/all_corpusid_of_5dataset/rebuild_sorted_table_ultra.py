#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重建表：超级优化版（按主键排序）
支持任意表的重建和排序
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
import db_config
import time
import argparse

# 表结构定义
TABLE_SCHEMAS = {
    'final_delivery': {
        'columns': 'id BIGINT, corpusid BIGINT NOT NULL, filename TEXT',
        'order_by': 'corpusid',
        'primary_key': 'id',
        'indexes': ['corpusid'],
        'insert_select': 'ROW_NUMBER() OVER (ORDER BY corpusid), corpusid, filename'
    },
    'abstracts': {
        'columns': 'corpusid BIGINT NOT NULL, data TEXT NOT NULL, insert_time TIMESTAMP(6), update_time TIMESTAMP(6)',
        'order_by': 'corpusid',
        'primary_key': 'corpusid',
        'indexes': [],
        'insert_select': 'corpusid, data, COALESCE(insert_time, CURRENT_TIMESTAMP), COALESCE(update_time, CURRENT_TIMESTAMP)'
    },
    'papers': {
        'columns': 'corpusid BIGINT NOT NULL, data TEXT NOT NULL, insert_time TIMESTAMP(6), update_time TIMESTAMP(6)',
        'order_by': 'corpusid',
        'primary_key': 'corpusid',
        'indexes': [],
        'insert_select': 'corpusid, data, COALESCE(insert_time, CURRENT_TIMESTAMP), COALESCE(update_time, CURRENT_TIMESTAMP)'
    }
}


def rebuild_ultra(table_name):
    """超级优化重建"""
    conn = None
    try:
        if table_name not in TABLE_SCHEMAS:
            print(f"[ERROR] 不支持的表: {table_name}")
            print(f"[*] 支持的表: {', '.join(TABLE_SCHEMAS.keys())}")
            return
        
        schema = TABLE_SCHEMAS[table_name]
        
        config = db_config.DB_CONFIG
        print(f"[*] Connecting to: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        conn.set_session(autocommit=False)
        
        cursor = conn.cursor()
        
        print(f"\n[*] Rebuild Table: {table_name}")
        print("="*70)
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_count = cursor.fetchone()[0]
        
        print(f"[*] Records: {total_count:,}")
        print(f"[*] Strategy: UNLOGGED + No Index + Ultra Params")
        
        response = input("\nContinue? (yes/no): ").strip().lower()
        if response != 'yes':
            print("[*] Cancelled")
            return
        
        overall_start = time.time()
        
        # Step 1: Clean up
        print("\n[Step 1/4] Cleaning...")
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}_sorted'
            );
        """)
        if cursor.fetchone()[0]:
            cursor.execute(f"DROP TABLE {table_name}_sorted CASCADE;")
            conn.commit()
        print("[+] Done")
        
        # Step 2: Set ultra params
        print("\n[Step 2/4] Setting ultra params...")
        ultra_params = {
            'maintenance_work_mem': '12GB',
            'work_mem': '8GB',
            'temp_buffers': '8GB',
            'max_parallel_workers_per_gather': '7',
            'max_parallel_maintenance_workers': '7',
            'effective_cache_size': '28GB',
            'synchronous_commit': 'off',
        }
        
        set_count = 0
        for name, value in ultra_params.items():
            try:
                cursor.execute(f"SET {name} = '{value}';")
                set_count += 1
            except Exception as e:
                conn.rollback()
                continue
        
        print(f"[+] Set {set_count}/{len(ultra_params)} params")
        
        # Step 3: Create and insert (no PK, ultra fast)
        print(f"\n[Step 3/4] Creating table and sorting (est. 20-40 min)...")
        print("[!] Key optimization: No PK/indexes during insert")
        step_start = time.time()
        
        # Create UNLOGGED table (fast)
        cursor.execute(f"""
            CREATE UNLOGGED TABLE {table_name}_sorted (
                {schema['columns']}
            ) WITH (fillfactor = 100);
        """)
        
        # Insert data sorted
        cursor.execute(f"""
            INSERT INTO {table_name}_sorted
            SELECT {schema['insert_select']}
            FROM {table_name}
            ORDER BY {schema['order_by']};
        """)
        conn.commit()
        
        step_elapsed = time.time() - step_start
        print(f"[+] Time: {step_elapsed:.1f}s ({step_elapsed/60:.1f}min)")
        print(f"[+] Speed: {total_count/step_elapsed:.0f} rows/s")
        
        # Step 4: Verify + Add constraints + Convert
        print("\n[Step 4/4] Verifying and adding constraints...")
        step_start = time.time()
        
        # Verify count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}_sorted;")
        new_count = cursor.fetchone()[0]
        
        if new_count != total_count:
            print(f"[ERROR] Count mismatch!")
            return
        
        print(f"[+] Verified: {new_count:,} rows")
        
        # Add primary key
        print("[*] Adding primary key...")
        cursor.execute(f"""
            ALTER TABLE {table_name}_sorted 
            ADD PRIMARY KEY ({schema['primary_key']});
        """)
        
        # Create additional indexes
        for idx_col in schema['indexes']:
            print(f"[*] Creating index on {idx_col}...")
            cursor.execute(f"""
                CREATE INDEX idx_{table_name}_sorted_{idx_col} 
                ON {table_name}_sorted({idx_col});
            """)
        
        conn.commit()
        
        # Convert to logged table
        print("[*] Converting to logged table...")
        cursor.execute(f"ALTER TABLE {table_name}_sorted SET LOGGED;")
        conn.commit()
        
        # Replace old table
        print("[*] Replacing old table...")
        cursor.execute(f"DROP TABLE {table_name} CASCADE;")
        cursor.execute(f"ALTER TABLE {table_name}_sorted RENAME TO {table_name};")
        cursor.execute(f"ALTER INDEX {table_name}_sorted_pkey RENAME TO {table_name}_pkey;")
        
        # Rename indexes
        for idx_col in schema['indexes']:
            cursor.execute(f"ALTER INDEX idx_{table_name}_sorted_{idx_col} RENAME TO idx_{table_name}_{idx_col};")
        
        conn.commit()
        
        # Update statistics
        cursor.execute(f"ANALYZE {table_name};")
        conn.commit()
        
        print(f"[+] Step 4 time: {time.time() - step_start:.1f}s")
        
        # Done
        overall_elapsed = time.time() - overall_start
        
        print("\n" + "="*70)
        print("[SUCCESS] Rebuild completed!")
        print("="*70)
        print(f"Records: {new_count:,}")
        print(f"Total time: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f}min)")
        print(f"Avg speed: {new_count/overall_elapsed:.0f} rows/s")
        print("="*70)
        
        cursor.close()
        conn.close()
        
    except KeyboardInterrupt:
        print("\n\n[!] User interrupted")
        if conn:
            conn.rollback()
            conn.close()
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Rebuild and sort PostgreSQL tables')
    parser.add_argument('table', nargs='?', default='final_delivery',
                       choices=list(TABLE_SCHEMAS.keys()),
                       help='Table name to rebuild (default: final_delivery)')
    
    args = parser.parse_args()
    rebuild_ultra(args.table)


if __name__ == '__main__':
    main()

