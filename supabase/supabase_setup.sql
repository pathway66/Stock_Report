-- ============================================================
-- AI+패스웨이 Supabase DB 스키마 v1.0
-- PRD v2 기반, 8개 테이블
-- ============================================================

-- 1. 일별 수급 데이터 (핵심)
CREATE TABLE daily_supply (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    market VARCHAR(10) NOT NULL,        -- KOSPI / KOSDAQ
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    subject VARCHAR(20) NOT NULL,       -- 외국인/연기금/투신/사모펀드/기타법인
    direction VARCHAR(10) NOT NULL,     -- 매수/매도
    quantity BIGINT DEFAULT 0,          -- 순매수/매도 수량 (백주)
    amount BIGINT DEFAULT 0,            -- 순매수/매도 금액 (백만원)
    avg_price BIGINT DEFAULT 0,         -- 추정평균가
    current_price BIGINT DEFAULT 0,     -- 현재가
    change_pct DECIMAL(10,2) DEFAULT 0, -- 대비율
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, stock_code, subject, direction)
);

-- 2. 일별 시세/시총
CREATE TABLE daily_market (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) DEFAULT '',      -- KOSPI / KOSDAQ
    close_price BIGINT DEFAULT 0,       -- 종가
    change_amount BIGINT DEFAULT 0,     -- 대비
    change_pct DECIMAL(10,2) DEFAULT 0, -- 등락률
    open_price BIGINT DEFAULT 0,        -- 시가
    high_price BIGINT DEFAULT 0,        -- 고가
    low_price BIGINT DEFAULT 0,         -- 저가
    volume BIGINT DEFAULT 0,            -- 거래량
    trade_value BIGINT DEFAULT 0,       -- 거래대금
    market_cap BIGINT DEFAULT 0,        -- 시가총액 (원)
    listed_shares BIGINT DEFAULT 0,     -- 상장주식수
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, stock_code)
);

-- 3. 분석 결과 (점수)
CREATE TABLE analysis_scores (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    sector VARCHAR(30) DEFAULT '기타',
    combo VARCHAR(30) DEFAULT '',       -- 외+연+투+사+기
    n_buyers INT DEFAULT 0,
    base_score DECIMAL(10,2) DEFAULT 0,
    ratio_score DECIMAL(10,2) DEFAULT 0,
    tushin_bonus DECIMAL(10,2) DEFAULT 0,
    conflict_penalty DECIMAL(10,2) DEFAULT 0,
    final_score DECIMAL(10,2) DEFAULT 0,
    net_ratio DECIMAL(10,4) DEFAULT 0,  -- net비중 %
    conflicts TEXT DEFAULT '',          -- 매도충돌 주체
    change_pct DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, stock_code)
);

-- 4. TOP3 이력 + 성과
CREATE TABLE top3_history (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,                 -- 선정일
    rank INT NOT NULL,                  -- 1, 2, 3
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    sector VARCHAR(30) DEFAULT '',
    score DECIMAL(10,2) DEFAULT 0,
    selection_price BIGINT DEFAULT 0,   -- 선정일 종가
    next_day_return DECIMAL(10,2),      -- 익일 수익률
    market_return DECIMAL(10,2),        -- 시장 수익률
    excess_return DECIMAL(10,2),        -- 초과수익률
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, rank)
);

-- 5. D전략 추적
CREATE TABLE d_strategy (
    id BIGSERIAL PRIMARY KEY,
    selection_date DATE NOT NULL,       -- 월요일 선정일
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    sector VARCHAR(30) DEFAULT '',
    combo VARCHAR(30) DEFAULT '',
    selection_price BIGINT DEFAULT 0,
    d1_return DECIMAL(10,2),
    d2_return DECIMAL(10,2),
    d3_return DECIMAL(10,2),
    d4_return DECIMAL(10,2),
    d5_return DECIMAL(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(selection_date, stock_code)
);

-- 6. 섹터맵
CREATE TABLE sector_map (
    id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL UNIQUE,
    stock_name VARCHAR(50) NOT NULL,
    market_cap BIGINT DEFAULT 0,        -- 시가총액 (억원)
    sector VARCHAR(30) NOT NULL,
    classifier VARCHAR(20) DEFAULT '',  -- 지니/자동/Shawn
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. 사용자 (구독 서비스용, Phase 4)
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(100),
    tier VARCHAR(20) DEFAULT 'free',    -- free / premium
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 8. 구독 (Phase 5)
CREATE TABLE subscriptions (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    plan VARCHAR(20) NOT NULL,          -- monthly / yearly
    status VARCHAR(20) DEFAULT 'active',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    payment_id VARCHAR(100)
);

-- 인덱스 (성능 최적화)
CREATE INDEX idx_supply_date ON daily_supply(date);
CREATE INDEX idx_supply_stock ON daily_supply(stock_code);
CREATE INDEX idx_supply_subject ON daily_supply(subject);
CREATE INDEX idx_market_date ON daily_market(date);
CREATE INDEX idx_market_stock ON daily_market(stock_code);
CREATE INDEX idx_scores_date ON analysis_scores(date);
CREATE INDEX idx_scores_final ON analysis_scores(final_score DESC);
CREATE INDEX idx_top3_date ON top3_history(date);
CREATE INDEX idx_sector_code ON sector_map(stock_code);
