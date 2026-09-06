-- CreateTable
CREATE TABLE "OhlvcData" (
    "ticket" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "open" DOUBLE PRECISION NOT NULL,
    "high" DOUBLE PRECISION NOT NULL,
    "low" DOUBLE PRECISION NOT NULL,
    "volume" DOUBLE PRECISION NOT NULL,
    "close" DOUBLE PRECISION NOT NULL
);

-- CreateTable
CREATE TABLE "Indicators" (
    "ticket" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "rsi" DOUBLE PRECISION NOT NULL,
    "ema" DOUBLE PRECISION NOT NULL,
    "sma" DOUBLE PRECISION NOT NULL
);

-- CreateIndex
CREATE UNIQUE INDEX "OhlvcData_ticket_key" ON "OhlvcData"("ticket");

-- CreateIndex
CREATE UNIQUE INDEX "Indicators_ticket_key" ON "Indicators"("ticket");
