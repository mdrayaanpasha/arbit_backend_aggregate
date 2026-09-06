/*
  Warnings:

  - You are about to drop the column `ticket` on the `Indicators` table. All the data in the column will be lost.
  - You are about to drop the column `ticket` on the `OhlvcData` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[ticker]` on the table `Indicators` will be added. If there are existing duplicate values, this will fail.
  - A unique constraint covering the columns `[ticker]` on the table `OhlvcData` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `ticker` to the `Indicators` table without a default value. This is not possible if the table is not empty.
  - Added the required column `ticker` to the `OhlvcData` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX "Indicators_ticket_key";

-- DropIndex
DROP INDEX "OhlvcData_ticket_key";

-- AlterTable
ALTER TABLE "Indicators" DROP COLUMN "ticket",
ADD COLUMN     "ticker" TEXT NOT NULL;

-- AlterTable
ALTER TABLE "OhlvcData" DROP COLUMN "ticket",
ADD COLUMN     "ticker" TEXT NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "Indicators_ticker_key" ON "Indicators"("ticker");

-- CreateIndex
CREATE UNIQUE INDEX "OhlvcData_ticker_key" ON "OhlvcData"("ticker");
