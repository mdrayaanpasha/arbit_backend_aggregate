import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const ONE_WEEK_AGO = () => new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

export async function OHLVCData(ticker, timestamp, open, high, low, volume, close) {
    await prisma.ohlvcData.deleteMany({
        where: { timestamp: { lt: ONE_WEEK_AGO() } }
    });

    return prisma.ohlvcData.create({
        data: { ticker, timestamp, open, high, low, volume, close }
    });
}

export async function Indicators(ticker, timestamp, rsi, ema, sma) {
    await prisma.indicators.deleteMany({
        where: { timestamp: { lt: ONE_WEEK_AGO() } }
    });

    return prisma.indicators.create({
        data: { ticker, timestamp, rsi, ema, sma }
    });
}