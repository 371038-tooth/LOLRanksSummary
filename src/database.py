import logging
import os
import asyncpg
from datetime import datetime, date, time
from src.utils.time_utils import get_now_jst, get_today_jst

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        # Support both custom URLs and Railway default URLs
        dsn = os.getenv('DATABASE_PUBLIC_URL') or os.getenv('DATABASE_URL')
        
        if dsn:
            # asyncpg requires 'postgresql://' scheme, some providers give 'postgres://'
            if dsn.startswith('postgres://'):
                dsn = dsn.replace('postgres://', 'postgresql://', 1)
            self.pool = await asyncpg.create_pool(dsn)
        else:
            # Support both 'DB_' and 'PG' prefixes (Railway uses PGxxx)
            self.pool = await asyncpg.create_pool(
                host=os.getenv('PGHOST') or os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('PGPORT') or os.getenv('DB_PORT', 5432)),
                user=os.getenv('PGUSER') or os.getenv('DB_USER', 'postgres'),
                password=os.getenv('PGPASSWORD') or os.getenv('DB_PASSWORD', 'password'),
                database=os.getenv('PGDATABASE') or os.getenv('DB_NAME', 'railway')
            )
        
        # Initialize schema
        await self.initialize()

    async def initialize(self):
        # Determine path to schema.sql relative to this file
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        schema_path = os.path.join(base_dir, 'schema.sql')
        
        if not os.path.exists(schema_path):
             if os.path.exists('schema.sql'):
                 schema_path = 'schema.sql'
        
        async with self.pool.acquire() as conn:
            # 1. Execute base schema if found
            if os.path.exists(schema_path):
                logger.info(f"Loading schema from {schema_path}")
                try:
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        schema_sql = f.read()
                        await conn.execute(schema_sql)
                except Exception as e:
                    logger.error(f"Error executing schema.sql: {e}")
            else:
                logger.warning(f"Warning: schema.sql not found at {schema_path}.")

            # 2. Independent Migration Steps
            
            # Step A: Add missing columns (Highest Priority)
            for table, column, col_type in [
                ('users', 'server_id', 'BIGINT'),
                ('rank_history', 'server_id', 'BIGINT'),
                ('rank_history', 'riot_id', 'VARCHAR(255)'),
                ('rank_history', 'wins', 'INTEGER DEFAULT 0'),
                ('rank_history', 'losses', 'INTEGER DEFAULT 0'),
                ('rank_history', 'games', 'INTEGER DEFAULT 0'),
                ('schedules', 'server_id', 'BIGINT'),
                ('schedules', 'status', 'VARCHAR(50) DEFAULT \'ENABLED\''),
                ('schedules', 'output_type', 'VARCHAR(50) DEFAULT \'table\''),
                ('schedules', 'period_type', 'VARCHAR(20) DEFAULT \'daily\''),
                ('schedules', 'split', 'BOOLEAN DEFAULT TRUE'),
                ('users', 'local_id', 'INTEGER'),
                ('schedules', 'local_id', 'INTEGER'),
            ]:
                try:
                    await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}")
                except Exception as e:
                    logger.warning(f"Could not add column {column} to {table}: {e}")

            # 既存スケジュールの graph 出力をデフォルトで分割表示にするマイグレーション
            try:
                await conn.execute("UPDATE schedules SET split = TRUE WHERE output_type = 'graph' AND split IS NULL")
            except Exception as e:
                logger.warning(f"Could not migrate split column: {e}")

            # Step B: Data Normalization
            try:
                await conn.execute("UPDATE users SET server_id = 0 WHERE server_id IS NULL")
                await conn.execute("UPDATE rank_history SET server_id = 0 WHERE server_id IS NULL")
                await conn.execute("UPDATE schedules SET server_id = 0 WHERE server_id IS NULL")
                
                # Initialize local_id for existing records
                users_without_id = await conn.fetch("SELECT server_id FROM users WHERE local_id IS NULL GROUP BY server_id")
                for row in users_without_id:
                    await self._reindex_users(row['server_id'], conn)
                
                schedules_without_id = await conn.fetch("SELECT server_id FROM schedules WHERE local_id IS NULL GROUP BY server_id")
                for row in schedules_without_id:
                    # Inline reindex for old schedules table (before migration to split tables)
                    sid = row['server_id']
                    sched_rows = await conn.fetch("SELECT id FROM schedules WHERE server_id = $1 ORDER BY id ASC", sid)
                    for i, sr in enumerate(sched_rows, 1):
                        await conn.execute("UPDATE schedules SET local_id = $1 WHERE id = $2", i, sr['id'])

            except Exception as e:
                logger.warning(f"Data normalization or local_id initialization failed: {e}")

            # Step C: Primary Key Migration (users)
            try:
                pk_check = await conn.fetch("""
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = 'users'::regclass AND i.indisprimary;
                """)
                pk_columns = [r['attname'] for r in pk_check]
                if 'server_id' not in pk_columns:
                    logger.info("Migrating 'users' Primary Key...")
                    await conn.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_pkey CASCADE")
                    await conn.execute("ALTER TABLE users ADD PRIMARY KEY (server_id, discord_id, riot_id)")
            except Exception as e:
                logger.warning(f"Users PK migration failed: {e}")

            # Step E: Migration to Split Schedule Tables
            try:
                # Check if old 'schedules' table exists
                schedules_exists = await conn.fetchval("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'schedules')")
                if schedules_exists:
                    logger.info("Starting schedule table migration...")
                    
                    # Migrate to schedules_table
                    await conn.execute("""
                        INSERT INTO schedules_table (server_id, local_id, schedule_time, channel_id, period_type, status, created_by, reg_date, update_date)
                        SELECT server_id, local_id, schedule_time, channel_id, period_type, status, created_by, reg_date, update_date
                        FROM schedules WHERE output_type = 'table'
                    """)
                    
                    # Migrate to schedules_graph
                    await conn.execute("""
                        INSERT INTO schedules_graph (server_id, local_id, schedule_time, channel_id, period_type, status, created_by, split, reg_date, update_date)
                        SELECT server_id, local_id, schedule_time, channel_id, period_type, status, created_by, split, reg_date, update_date
                        FROM schedules WHERE output_type = 'graph'
                    """)
                    
                    # Re-index because we split them, local_id might conflict or have gaps per table
                    table_servers = await conn.fetch("SELECT server_id FROM schedules_table GROUP BY server_id")
                    for s in table_servers:
                        await self._do_reindex_schedules_table(s['server_id'], conn)
                    
                    graph_servers = await conn.fetch("SELECT server_id FROM schedules_graph GROUP BY server_id")
                    for s in graph_servers:
                        await self._do_reindex_schedules_graph(s['server_id'], conn)

                    # Rename old table for backup instead of deleting? User asked for migration.
                    # Let's keep it as schedules_backup for safety if we can.
                    await conn.execute("ALTER TABLE schedules RENAME TO schedules_old_backup")
                    logger.info("Schedule table migration completed. Old table renamed to schedules_old_backup.")
            except Exception as e:
                logger.warning(f"Schedule table migration failed: {e}")

        logger.info("Database initialization and migration check completed.")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def register_user(self, server_id: int, discord_id: int, riot_id: str, puuid: str):
        async with self.pool.acquire() as conn:
            if not await conn.fetchrow("SELECT 1 FROM users WHERE server_id = $1 AND discord_id = $2 AND riot_id = $3", server_id, discord_id, riot_id):
                # New registration, assign local_id
                max_id = await conn.fetchval("SELECT MAX(local_id) FROM users WHERE server_id = $1", server_id) or 0
                local_id = max_id + 1
                now = get_now_jst()
                query = """
                INSERT INTO users (server_id, discord_id, riot_id, puuid, local_id, reg_date, update_date)
                VALUES ($1, $2, $3, $4, $5, $6, $6)
                """
                await conn.execute(query, server_id, discord_id, riot_id, puuid, local_id, now)
            else:
                # Update existing
                query = """
                UPDATE users SET puuid = $4, update_date = $5
                WHERE server_id = $1 AND discord_id = $2 AND riot_id = $3
                """
                await conn.execute(query, server_id, discord_id, riot_id, puuid, get_now_jst())

    async def get_user_by_discord_id(self, server_id: int, discord_id: int):
        query = "SELECT * FROM users WHERE server_id = $1 AND discord_id = $2"
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, server_id, discord_id)

    async def get_user_by_riot_id(self, server_id: int, riot_id: str):
        """Fetch a user by their Riot ID within a specific server."""
        query = "SELECT * FROM users WHERE server_id = $1 AND riot_id = $2"
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, server_id, riot_id)

    async def register_schedule(self, server_id: int, schedule_time, channel_id: int, created_by: int, period_type: str, output_type: str = 'table', split: bool = True):
        if isinstance(schedule_time, str):
            try:
                if len(schedule_time.split(':')) == 2:
                    dt = datetime.strptime(schedule_time, "%H:%M")
                else:
                    dt = datetime.strptime(schedule_time, "%H:%M:%S")
                schedule_time = dt.time()
            except ValueError as e:
                raise ValueError(f"Invalid time format: {schedule_time}") from e

        async with self.pool.acquire() as conn:
            if output_type == 'table':
                max_id = await conn.fetchval("SELECT MAX(local_id) FROM schedules_table WHERE server_id = $1", server_id) or 0
                local_id = max_id + 1
                query = """
                INSERT INTO schedules_table (server_id, schedule_time, channel_id, created_by, period_type, status, local_id, reg_date, update_date)
                VALUES ($1, $2, $3, $4, $5, 'ENABLED', $6, $7, $7)
                RETURNING local_id
                """
                return await conn.fetchval(query, server_id, schedule_time, channel_id, created_by, period_type, local_id, get_now_jst())
            else:
                max_id = await conn.fetchval("SELECT MAX(local_id) FROM schedules_graph WHERE server_id = $1", server_id) or 0
                local_id = max_id + 1
                query = """
                INSERT INTO schedules_graph (server_id, schedule_time, channel_id, created_by, period_type, split, status, local_id, reg_date, update_date)
                VALUES ($1, $2, $3, $4, $5, $6, 'ENABLED', $7, $8, $8)
                RETURNING local_id
                """
                return await conn.fetchval(query, server_id, schedule_time, channel_id, created_by, period_type, split, local_id, get_now_jst())

    async def get_all_schedules(self):
        async with self.pool.acquire() as conn:
            tables = await conn.fetch("SELECT *, 'table' as output_type, NULL as split FROM schedules_table")
            graphs = await conn.fetch("SELECT *, 'graph' as output_type FROM schedules_graph")
            combined = [dict(r) for r in tables] + [dict(r) for r in graphs]
            return combined

    async def get_schedules_by_server(self, server_id: int):
        async with self.pool.acquire() as conn:
            tables = await conn.fetch("SELECT *, 'table' as output_type, NULL as split FROM schedules_table WHERE server_id = $1", server_id)
            graphs = await conn.fetch("SELECT *, 'graph' as output_type FROM schedules_graph WHERE server_id = $1", server_id)
            combined = [dict(r) for r in tables] + [dict(r) for r in graphs]
            # Maintain some sense of local_id order across both if possible, or just concat
            return sorted(combined, key=lambda x: x['local_id'])

    async def add_rank_history(self, server_id: int, discord_id: int, riot_id: str, tier: str, rank: str, lp: int, wins: int, losses: int, fetch_date: date, reg_date=None):
        if reg_date is None:
            reg_date = get_now_jst()
        games = (wins or 0) + (losses or 0)
        query = """
        INSERT INTO rank_history (server_id, discord_id, riot_id, tier, rank, lp, wins, losses, games, fetch_date, reg_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (server_id, discord_id, riot_id, fetch_date)
        DO UPDATE SET 
            tier = $4, rank = $5, lp = $6, wins = $7, losses = $8, games = $9, 
            reg_date = $11
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, server_id, discord_id, riot_id, tier, rank, lp, wins, losses, games, fetch_date, reg_date)

    async def get_rank_history(self, server_id: int, discord_id: int, riot_id: str, start_date: date, end_date: date):
        query = """
        SELECT * FROM rank_history
        WHERE server_id = $1 AND discord_id = $2 AND riot_id = $3 AND fetch_date BETWEEN $4 AND $5
        ORDER BY fetch_date ASC
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, server_id, discord_id, riot_id, start_date, end_date)

    async def get_rank_history_for_graph(self, server_id: int, discord_id: int, riot_id: str, start_date: date):
        query = """
        SELECT fetch_date, tier, rank, lp, wins, losses, games, reg_date
        FROM rank_history
        WHERE server_id = $1 AND discord_id = $2 AND riot_id = $3 AND fetch_date >= $4
        ORDER BY fetch_date ASC
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, server_id, discord_id, riot_id, start_date)

    async def get_all_users(self):
        query = "SELECT * FROM users ORDER BY server_id, local_id ASC"
        async with self.pool.acquire() as conn:
            return await conn.fetch(query)

    async def get_users_by_server(self, server_id: int):
        query = "SELECT * FROM users WHERE server_id = $1 ORDER BY local_id ASC"
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, server_id)

    async def delete_schedule(self, server_id: int, local_id: int, output_type: str):
        async with self.pool.acquire() as conn:
            if output_type == 'table':
                await conn.execute("DELETE FROM schedules_table WHERE server_id = $1 AND local_id = $2", server_id, local_id)
                await self._do_reindex_schedules_table(server_id, conn)
            else:
                await conn.execute("DELETE FROM schedules_graph WHERE server_id = $1 AND local_id = $2", server_id, local_id)
                await self._do_reindex_schedules_graph(server_id, conn)

    async def _do_reindex_schedules_table(self, server_id: int, conn):
        rows = await conn.fetch("SELECT id FROM schedules_table WHERE server_id = $1 ORDER BY id ASC", server_id)
        for i, row in enumerate(rows, 1):
            await conn.execute("UPDATE schedules_table SET local_id = $1 WHERE id = $2", i, row['id'])

    async def _do_reindex_schedules_graph(self, server_id: int, conn):
        rows = await conn.fetch("SELECT id FROM schedules_graph WHERE server_id = $1 ORDER BY id ASC", server_id)
        for i, row in enumerate(rows, 1):
            await conn.execute("UPDATE schedules_graph SET local_id = $1 WHERE id = $2", i, row['id'])

    async def update_schedule(self, server_id: int, local_id: int, output_type: str, schedule_time, channel_id: int, period_type: str, split: bool = True):
        if isinstance(schedule_time, str):
            try:
                if len(schedule_time.split(':')) == 2:
                    dt = datetime.strptime(schedule_time, "%H:%M")
                else:
                    dt = datetime.strptime(schedule_time, "%H:%M:%S")
                schedule_time = dt.time()
            except ValueError as e:
                raise ValueError(f"Invalid time format: {schedule_time}") from e

        async with self.pool.acquire() as conn:
            if output_type == 'table':
                query = """
                UPDATE schedules_table 
                SET schedule_time = $3, channel_id = $4, period_type = $5, update_date = $6
                WHERE server_id = $1 AND local_id = $2
                """
                await conn.execute(query, server_id, local_id, schedule_time, channel_id, period_type, get_now_jst())
            else:
                query = """
                UPDATE schedules_graph 
                SET schedule_time = $3, channel_id = $4, period_type = $5, split = $6, update_date = $7
                WHERE server_id = $1 AND local_id = $2
                """
                await conn.execute(query, server_id, local_id, schedule_time, channel_id, period_type, split, get_now_jst())

    async def set_schedule_status(self, server_id: int, local_id: int, output_type: str, status: str):
        async with self.pool.acquire() as conn:
            table = "schedules_table" if output_type == 'table' else "schedules_graph"
            query = f"""
            UPDATE {table} 
            SET status = $3, update_date = $4
            WHERE server_id = $1 AND local_id = $2
            """
            await conn.execute(query, server_id, local_id, status, get_now_jst())

    async def get_schedule_by_id(self, server_id: int, local_id: int, output_type: str):
        table = "schedules_table" if output_type == 'table' else "schedules_graph"
        query = f"SELECT *, '{output_type}' as output_type FROM {table} WHERE server_id = $1 AND local_id = $2"
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, server_id, local_id)

    async def delete_user_by_local_id(self, server_id: int, local_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE server_id = $1 AND local_id = $2", server_id, local_id)
            await self._reindex_users(server_id, conn)

    async def _reindex_users(self, server_id: int, conn=None):
        if conn:
            await self._do_reindex_users(server_id, conn)
        else:
            async with self.pool.acquire() as conn:
                await self._do_reindex_users(server_id, conn)

    async def _do_reindex_users(self, server_id: int, conn):
        # We need a stable ordering for re-indexing. Using reg_date or riot_id.
        rows = await conn.fetch("SELECT discord_id, riot_id FROM users WHERE server_id = $1 ORDER BY reg_date ASC, riot_id ASC", server_id)
        for i, row in enumerate(rows, 1):
            await conn.execute("UPDATE users SET local_id = $1 WHERE server_id = $2 AND discord_id = $3 AND riot_id = $4", i, server_id, row['discord_id'], row['riot_id'])

    async def delete_user_by_riot_id(self, server_id: int, riot_id: str):
        query = "DELETE FROM users WHERE server_id = $1 AND riot_id = $2"
        async with self.pool.acquire() as conn:
            await conn.execute(query, server_id, riot_id)

db = Database()
