const express = require("express");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, "public")));

const server = app.listen(PORT, "0.0.0.0", () => {
    console.log( `FINISHER running on port ${PORT} `);
});

const dashboard = new WebSocket.Server({ server });

const DERIV_URL = "wss://api.derivws.com/trading/v1/options/ws/public";

const ALLOWED_MARKETS = [
    "1HZ10V",
    "1HZ15V",
    "1HZ25V",
    "1HZ30V",
    "1HZ50V",
    "1HZ75V",
    "1HZ90V",
    "1HZ100V"
];

let market = "1HZ10V";
let deriv = null;
let subscriptionId = null;

let ticks = [];
let analysing = false;
let predictionLocked = false;
let tradeNowLocked = false;

let predictionDigit = null;
let predictionPercent = null;
let hiddenEntryDigit = null;
let entryPercent = null;

let reconnectTimer = null;
let reconnectDelay = 2000;
let requestId = 1;

let latestAnalysis = {
    type: "analysis",
    market,
    active: false,
    tickCount: 0,
    predictionDigit: null,
    predictionPercent: null,
    entryDigit: null,
    entryPercent: null,
    signal: "IDLE"
};

function broadcast(data) {
    const message = JSON.stringify(data);

    dashboard.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

function sendAnalysis(signal) {
    latestAnalysis = {
        type: "analysis",
        market,
        active: analysing,
        tickCount: Math.min(ticks.length, 30),
        predictionDigit,
predictionPercent,
entryDigit: hiddenEntryDigit,
entryPercent,
predictionLocked,
        tradeNowLocked,
        signal
    };

    broadcast(latestAnalysis);
}

function resetCycle() {
    ticks = [];
    analysing = false;
    predictionLocked = false;
    tradeNowLocked = false;

    predictionDigit = null;
    predictionPercent = null;
    hiddenEntryDigit = null;
    entryPercent = null;

    sendAnalysis("IDLE");
}

function startAnalysis() {
    ticks = [];
    analysing = true;
    predictionLocked = false;
    tradeNowLocked = false;

    predictionDigit = null;
    predictionPercent = null;
    hiddenEntryDigit = null;
    entryPercent = null;

    sendAnalysis("SCANNING");

    console.log("New analysis started on", market);
}

function connectDeriv() {
    clearTimeout(reconnectTimer);

    console.log("Connecting to Deriv public tick feed...");

    deriv = new WebSocket(DERIV_URL);

    deriv.on("open", () => {
        console.log("Deriv connection established");

        reconnectDelay = 2000;

        broadcast({
            type: "status",
            connected: true
        });

        subscribeToMarket();
    });

    deriv.on("message", raw => {
        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (data.error) {
            console.log("Deriv error:", data.error.message);
            return;
        }

        if (data.subscription && data.subscription.id) {
            subscriptionId = data.subscription.id;
        }

        if (data.msg_type === "tick" && data.tick) {
            processTick(data.tick);
        }
    });

    deriv.on("close", () => {
        console.log("Deriv connection closed");

        subscriptionId = null;

        broadcast({
            type: "status",
            connected: false
        });

        scheduleReconnect();
    });

    deriv.on("error", error => {
        console.log("Deriv connection error:", error.message);
    });
}

function scheduleReconnect() {
    clearTimeout(reconnectTimer);

    reconnectTimer = setTimeout(() => {
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
        connectDeriv();
    }, reconnectDelay);
}

function subscribeToMarket() {
    if (!deriv || deriv.readyState !== WebSocket.OPEN) {
        return;
    }

    deriv.send(JSON.stringify({
        ticks: market,
        subscribe: 1,
        req_id: requestId++
    }));

    console.log("Subscribed to", market);
}

function changeMarket(newMarket) {
    if (!ALLOWED_MARKETS.includes(newMarket)) {
        return;
    }

    if (newMarket === market) {
        resetCycle();
        return;
    }

    if (
        subscriptionId &&
        deriv &&
        deriv.readyState === WebSocket.OPEN
    ) {
        deriv.send(JSON.stringify({
            forget: subscriptionId,
            req_id: requestId++
        }));
    }

    subscriptionId = null;
    market = newMarket;

    resetCycle();
    subscribeToMarket();

    console.log("Market changed to", market);
}

function getDigitCounts(values) {
    const counts = Array(10).fill(0);

    values.forEach(digit => {
        counts[digit]++;
    });

    return counts;
}

function getLastDigit(tick) {
    const quote = Number(tick.quote);

    if (!Number.isFinite(quote)) {
        return null;
    }

    const decimalPlaces = Number.isInteger(tick.pip_size)
        ? tick.pip_size
        : 2;

    const formatted = quote.toFixed(decimalPlaces);
    const digit = Number(formatted.slice(-1));

    if (!(Number.isInteger(digit) && digit >= 0 && digit <= 9)) {
        return null;
    }

    return digit;
}

function processTick(tick) {
    const digit = getLastDigit(tick);

    if (digit === null) {
    return;
}
if (!analysing) {
    return;
}
if (tradeNowLocked) {
    return;
}

    // Once the prediction is locked, only watch for the
    // already-qualified hidden entry digit.
    // The current incoming tick is checked before updating
    // the rolling analysis window.
    if (predictionLocked) {
        if (digit === hiddenEntryDigit) {
            tradeNowLocked = true;

            console.log("TRADE NOW locked on", market);

            sendAnalysis("TRADE_NOW");
            return;
        }

        ticks.push(digit);

        if (ticks.length > 30) {
            ticks.shift();
        }

        sendAnalysis("WAITING");
        return;
    }

    // Collect the first 30 ticks.
    ticks.push(digit);

    if (ticks.length > 30) {
        ticks.shift();
    }

    if (ticks.length < 30) {
        sendAnalysis("SCANNING");
        return;
    }

    const counts = getDigitCounts(ticks);

    const minimum = Math.min(...counts);
    const maximum = Math.max(...counts);

    // Lowest digit wins a tie for the least frequent digit.
    const leastDigit = counts.findIndex(count => count === minimum);

    // The most frequent digit must be unique.
    const mostDigits = counts
        .map((count, index) => ({
            digit: index,
            count
        }))
        .filter(item => item.count === maximum);

    const predictionQualifies =
    minimum <= 1;

const entryQualifies =
    maximum >= 7 && mostDigits.length === 1;

    // Lock both conditions together.
    if (predictionQualifies && entryQualifies) {
        predictionDigit = leastDigit;
        predictionPercent = minimum;
hiddenEntryDigit = mostDigits[0].digit;
entryPercent = (maximum / 30) * 100;

        predictionLocked = true;

        console.log(
            "Prediction locked:",
            predictionDigit,
            "Entry condition qualified."
        );

        sendAnalysis("WAITING");
        return;
    }

    sendAnalysis("ANALYSING");
}

dashboard.on("connection", client => {
    client.send(JSON.stringify({
        type: "status",
        connected: deriv?.readyState === WebSocket.OPEN
    }));

    client.send(JSON.stringify(latestAnalysis));

    client.on("message", raw => {
        let message;

        try {
            message = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (
            message.action === "subscribe" &&
            ALLOWED_MARKETS.includes(message.market)
        ) {
            changeMarket(message.market);
            return;
        }

        if (message.action === "analyse") {
            startAnalysis();
        }
    });
});

connectDeriv();

process.on("SIGINT", () => {
    clearTimeout(reconnectTimer);

    if (deriv) {
        deriv.close();
    }

    server.close(() => {
        console.log("FINISHER stopped");
        process.exit(0);
    });
});