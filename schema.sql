-- Select the new UTC database before applying this file.
CREATE TABLE IF NOT EXISTS k (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(32) CHARACTER SET ascii NOT NULL,
    time_interval VARCHAR(8) CHARACTER SET ascii NOT NULL,
    candle_begin_time DATETIME NOT NULL COMMENT 'UTC open time',
    open   DECIMAL(38,18) NOT NULL,
    high   DECIMAL(38,18) NOT NULL,
    low    DECIMAL(38,18) NOT NULL,
    close  DECIMAL(38,18) NOT NULL,
    volume DECIMAL(38,18) NOT NULL,
    quote_volume                 DECIMAL(38,18) NOT NULL,
    trade_num                    BIGINT UNSIGNED NOT NULL,
    taker_buy_base_asset_volume  DECIMAL(38,18) NOT NULL,
    taker_buy_quote_asset_volume DECIMAL(38,18) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_kline (symbol, time_interval, candle_begin_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
