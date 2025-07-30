const axios = require('axios');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const nodemailer = require('nodemailer');
const cron = require('node-cron');

dayjs.extend(utc);

const BASE_URL = 'https://api-futures.kucoin.com/api/v1';
const SPIKE_RATIO = 2;

const EMAIL_FROM = 'puriyasadat@gmail.com';
const EMAIL_PASS = 'ozrq fpiy zsgw fybw';
const EMAIL_TO = 'sadat.pouria@gmail.com';

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: EMAIL_FROM,
    pass: EMAIL_PASS,
  },
});

async function sendEmail(subject, bodyText, bodyHtml) {
  try {
    await transporter.sendMail({ from: EMAIL_FROM, to: EMAIL_TO, subject, text: bodyText, html: bodyHtml });
    console.log(`📨 Email sent: ${subject}`);
  } catch (err) {
    console.error('❌ Email error:', err.message);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let globalPause = Promise.resolve();

async function pauseAll(ms) {
  console.warn(`⏸️ Global pause for ${ms / 1000} seconds due to rate limiting...`);
  globalPause = sleep(ms);
  await globalPause;
}

async function retryOnRateLimit(fn, args, label) {
  try {
    await globalPause;
    return await fn(...args);
  } catch (err) {
    if (err.response && err.response.status === 429) {
      console.warn(`⏳ Rate limit hit for ${label}. Initiating global pause...`);
      globalPause = sleep(31000);
      await globalPause;
      try {
        return await fn(...args);
      } catch (err2) {
        console.warn(`❌ Retry failed for ${label}:`, err2.message);
        return null;
      }
    } else {
      console.warn(`❌ ${label} failed:`, err.message);
      return null;
    }
  }
}

async function fetchSymbols() {
  try {
    const { data } = await axios.get(`${BASE_URL}/contracts/active`);
    return data.data
      .map(c => ({ fullSymbol: c.symbol, baseSymbol: `K${c.baseCurrency}` }))
      .sort((a, b) => a.fullSymbol.localeCompare(b.fullSymbol));
  } catch (err) {
    console.error('❌ Failed to fetch symbols:', err.message);
    return [];
  }
}

async function fetchHistoricalVolumes(symbol) {
  const calls = [];
  for (let i = 0; i < 8; i++) {
    const from = dayjs.utc().startOf('day').subtract(((i + 1) * 100) + 1, 'day').valueOf();
    const to = dayjs.utc().startOf('day').subtract((i * 100) + 2, 'day').valueOf();
    const url = `${BASE_URL}/kline/query?symbol=${symbol}&granularity=1440&from=${from}&to=${to}`;

    const fetch = async () => {
      const { data } = await axios.get(url);
      return data.data
        .map(k => ({ date: dayjs.utc(k[0]).format('YYYY-MM-DD'), volume: parseFloat(k[5]) }))
        .filter(v => !isNaN(v.volume) && v.volume > 0);
    };

    calls.push(retryOnRateLimit(fetch, [], `historical volumes [${i + 1}] for ${symbol}`));
  }

  const chunks = await Promise.all(calls);
  const flattened = chunks.flat().filter(Boolean);
  if (flattened.length > 0) {
    console.log(`📊 ${symbol}: Using ${flattened.length} daily volumes`);
    return flattened;
  }
  return null;
}

async function fetchRolling24hVolume(symbol) {
  const start = dayjs.utc().startOf('day').subtract(1, 'day').valueOf();
  const end = dayjs.utc().startOf('day').valueOf();
  const url = `${BASE_URL}/kline/query?symbol=${symbol}&granularity=1440&from=${start}&to=${end}`;

  const fetch = async () => {
    const { data } = await axios.get(url);
    return data.data.map(k => parseFloat(k[5])).filter(v => !isNaN(v) && v > 0).reduce((acc, v) => acc + v, 0);
  };

  return retryOnRateLimit(fetch, [], `rolling 24h volume for ${symbol}`);
}

async function checkVolumeSpike(symbol, baseSymbol) {
  const [volumeData, todayVolume] = await Promise.all([
    fetchHistoricalVolumes(symbol),
    fetchRolling24hVolume(symbol),
  ]);

  if (!volumeData || volumeData.length < 10) return;

  const prevMaxEntry = volumeData.reduce((max, curr) => curr.volume > max.volume ? curr : max);

  const prevMax = prevMaxEntry.volume;
  const prevMaxDate = prevMaxEntry.date;

  console.log(`${symbol} todayVolume: ${todayVolume}, prevMax: ${prevMax}`);

  if (todayVolume > SPIKE_RATIO * prevMax) {
    const ratio = (todayVolume / prevMax).toFixed(2);
    const todayDate = dayjs().format('YYYY-MM-DD');
    const subject = `🚀 Volume Spike: ${symbol} (${ratio}x)`;

    const bodyText = `Coin: ${symbol}\nDate: ${todayDate}\nToday's Volume: ${todayVolume.toLocaleString()}\nPrevious Max Volume: ${prevMax.toLocaleString()} (on ${prevMaxDate})\nSpike Ratio: ${ratio}x`;

    const bodyHtml = `
      <h2>🚀 Volume Spike Detected</h2>
      <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
        <thead>
          <tr style="background-color: #f2f2f2;">
            <th colspan="3" style="text-align: center; font-size: 20px; font-weight: bold;">${symbol}</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Today</strong></td>
            <td>${todayVolume.toLocaleString()}</td>
            <td>${todayDate}</td>
          </tr>
          <tr>
            <td><strong>Previous Max</strong></td>
            <td>${prevMax.toLocaleString()}</td>
            <td>${prevMaxDate}</td>
          </tr>
          <tr>
            <td><strong>Spike Ratio</strong></td>
            <td colspan="2" style="text-align: center;"><strong>${ratio}x</strong></td>
          </tr>
        </tbody>
      </table>
    `;

    console.log(subject + '\n' + bodyText);
    await sendEmail(subject, bodyText, bodyHtml);
  }
}

async function runVolumeCheck() {
  const { default: pLimit } = await import('p-limit');
  const limit = pLimit(2);
  const SLEEP_BETWEEN_BATCHES_MS = 10000;

  const symbols = await fetchSymbols();
  console.log(`🔍 Checking ${symbols.length} symbols...\n`);

  // Helper sleep function
  const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

  // Process in batches of 5
  for (let i = 0; i < symbols.length; i += 22) {
    const batch = symbols.slice(i, i + 22);
    const tasks = batch.map(({ fullSymbol, baseSymbol }) =>
      limit(() => checkVolumeSpike(fullSymbol, baseSymbol))
    );

    await Promise.all(tasks);

    // Avoid sleeping after the last batch
    if (i + 5 < symbols.length) {
      console.log(`⏳ Sleeping ${SLEEP_BETWEEN_BATCHES_MS / 1000}s before next batch...`);
      await sleep(SLEEP_BETWEEN_BATCHES_MS);
    }
  }

  console.log('✅ Volume check complete.');
}

cron.schedule('0 0 * * *', () => {
  console.log(`🕔 Scheduled volume check at ${new Date().toISOString()}`);
  runVolumeCheck();
});

runVolumeCheck();
