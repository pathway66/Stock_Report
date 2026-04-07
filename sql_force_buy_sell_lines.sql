-- ============================================================
-- force_buy_sell_lines 테이블 생성
-- AI+패스웨이 주도세력 매수선/매도선 캐시
-- Supabase SQL Editor에서 실행
-- ============================================================

CREATE TABLE IF NOT EXISTS force_buy_sell_lines (
  id BIGSERIAL PRIMARY KEY,

  -- 기본 정보
  date DATE NOT NULL,
  stock_code VARCHAR(10) NOT NULL,
  stock_name VARCHAR(50),
  market VARCHAR(10),              -- KOSPI / KOSDAQ

  -- 주도 매수세력
  dominant_buyer VARCHAR(20),      -- 주도 매수주체 (외국인/연기금/투신/사모펀드/기타법인)
  dominant_buyer_pct NUMERIC,      -- 주도 매수주체 비중 (%)
  sub_buyer VARCHAR(20),           -- 보조 매수주체
  sub_buyer_pct NUMERIC,           -- 보조 매수주체 비중 (%)

  -- 주도 매도세력
  dominant_seller VARCHAR(20),     -- 주도 매도주체
  dominant_seller_pct NUMERIC,     -- 주도 매도주체 비중 (%)

  -- 세력매수선 (Support Buy lines)
  sb5 NUMERIC,                     -- 5일 세력매수선 (단기 지지)
  sb20 NUMERIC,                    -- 20일 세력매수선 (핵심 지지)
  sb60 NUMERIC,                    -- 60일 세력매수선 (최종 방어선)

  -- 세력매도선 (Supply Sell lines)
  ss5 NUMERIC,                     -- 5일 세력매도선 (단기 저항)
  ss20 NUMERIC,                    -- 20일 세력매도선 (핵심 저항)
  ss60 NUMERIC,                    -- 60일 세력매도선 (최종 저항)

  -- 매매 시그널
  signal_status VARCHAR(20),       -- buy_zone / warning / danger / exit / overheated / momentum_weak / force_switch / breakout
  signal_detail JSONB DEFAULT '{}', -- 상세 시그널 데이터

  -- 부가 정보
  data_days INT,                   -- 분석에 사용된 거래일수
  consecutive_buy_days INT,        -- 주도매수주체 연속매수일수

  created_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(date, stock_code)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_fbsl_date ON force_buy_sell_lines(date);
CREATE INDEX IF NOT EXISTS idx_fbsl_stock ON force_buy_sell_lines(stock_code);
CREATE INDEX IF NOT EXISTS idx_fbsl_signal ON force_buy_sell_lines(signal_status);
CREATE INDEX IF NOT EXISTS idx_fbsl_buyer ON force_buy_sell_lines(dominant_buyer);

-- RLS (Row Level Security) - 읽기만 허용
ALTER TABLE force_buy_sell_lines ENABLE ROW LEVEL SECURITY;

CREATE POLICY "force_buy_sell_lines_select"
  ON force_buy_sell_lines FOR SELECT
  USING (true);

CREATE POLICY "force_buy_sell_lines_insert"
  ON force_buy_sell_lines FOR INSERT
  WITH CHECK (true);

CREATE POLICY "force_buy_sell_lines_update"
  ON force_buy_sell_lines FOR UPDATE
  USING (true);

-- 스키마 캐시 갱신
NOTIFY pgrst, 'reload schema';

-- 확인
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'force_buy_sell_lines' ORDER BY ordinal_position;
