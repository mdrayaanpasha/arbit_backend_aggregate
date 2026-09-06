import YahooFinance from 'yahoo-finance2';
const yahooFinance = new YahooFinance();

export const tickers = [
  'AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','BRK-B','JPM','V',
  'UNH','XOM','JNJ','WMT','MA','PG','LLY','CVX','HD','MRK',
  'ABBV','PEP','KO','AVGO','COST','ADBE','CSCO','TMO','ACN','MCD',
  'BAC','NKE','DHR','TXN','NEE','PM','UPS','MS','INTC','AMGN',
  'RTX','SCHW','INTU','QCOM','IBM','CAT','SPGI','GS','BLK','AXP',
  'ISRG','AMD','GILD','DE','SYK','ADP','PLD','MDLZ','ADI','REGN',
  'VRTX','MO','ZTS','CI','TJX','EOG','SO','DUK','NOC','MMC',
  'ITW','BSX','HCA','SLB','APD','PGR','CME','ETN','EW','AON',
  'BDX','FISV','MCO','KLAC','NSC','EMR','MET','PSA','WM','F',
  'GM','UBER','ABNB','SNAP','SHOP','SQ','PLTR','COIN','RBLX','HOOD'
];

async function fetchOHLCV(ticker) {
  try {
    const data = await yahooFinance.historical(ticker, {
      period1: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000), // 7 days — historical() is daily candles, 5min window returns nothing
      period2: new Date(),
    });

    return data.map(e => ({
      ticker,
      date:   e.date,
      open:   e.open,
      high:   e.high,
      low:    e.low,
      close:  e.close,
      volume: e.volume,
    }));
  } catch (err) {
    console.error(`Failed: ${ticker}`, err.message);
    return [];
  }
}

export async function fetchAll(batchSize = 10) {
  const results = [];

  for (let i = 0; i < tickers.length; i += batchSize) {
    const batch = tickers.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(fetchOHLCV));
    results.push(...batchResults.flat());
    console.log(`Fetched ${Math.min(i + batchSize, tickers.length)}/${tickers.length}`);
    await new Promise(r => setTimeout(r, 500));
  }

  return results;
}