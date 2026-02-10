CREATE TABLE IF NOT EXISTS users (
    server_id BIGINT,
    local_id INTEGER,
    discord_id BIGINT,
    riot_id VARCHAR(255) NOT NULL,
    puuid VARCHAR(255) NOT NULL,
    reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (server_id, discord_id, riot_id)
);

CREATE TABLE IF NOT EXISTS rank_history (
    id SERIAL PRIMARY KEY,
    server_id BIGINT,
    discord_id BIGINT,
    riot_id VARCHAR(255),
    tier VARCHAR(50),
    rank VARCHAR(10),
    lp INTEGER,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    games INTEGER DEFAULT 0,
    fetch_date DATE NOT NULL,
    reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (server_id, discord_id, riot_id) REFERENCES users(server_id, discord_id, riot_id) ON DELETE CASCADE,
    UNIQUE (server_id, discord_id, riot_id, fetch_date)
);

CREATE TABLE IF NOT EXISTS schedules (
    id SERIAL PRIMARY KEY,
    server_id BIGINT,
    local_id INTEGER,
    schedule_time TIME NOT NULL,
    channel_id BIGINT NOT NULL,
    period_type VARCHAR(20) DEFAULT 'daily',
    status VARCHAR(50) DEFAULT 'ENABLED',
    output_type VARCHAR(50) DEFAULT 'table',
    created_by BIGINT,
    reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
