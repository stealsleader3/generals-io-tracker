const express = require("express");
const http = require("http");
const path = require("path");
const io = require("socket.io-client");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// Serve static files
app.use(express.static(path.join(__dirname, "public")));

// ========== generals.io Data Fetching ==========

const GENERALS_WS_URL = "https://ws.generals.io";
const GENERALS_API_BASE = "https://generals.io/api";
const LADDER_KEY = "duel"; // 1v1 ladder key
const PLAYER_COUNT = 50; // Top 50 players

let cachedLeaderboard = null;
let lastUpdateTime = null;
let isFetching = false;
let socket = null;

// In-memory cache for player last-game data
const playerGameCache = new Map(); // username -> { lastGameTime, fetchedAt }

/**
 * Connect to generals.io Socket.IO and fetch the 1v1 leaderboard.
 */
function fetchLeaderboardViaSocket() {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Socket.IO leaderboard timeout"));
      if (socket) {
        socket.disconnect();
        socket = null;
      }
    }, 15000);

    try {
      socket = io(GENERALS_WS_URL, {
        transports: ["websocket"],
        forceNew: true,
      });

      socket.on("connect", () => {
        console.log("[Socket.IO] Connected to generals.io");

        // Request the 1v1 leaderboard
        socket.emit("leaderboard", LADDER_KEY, (data) => {
          clearTimeout(timeout);
          if (data) {
            console.log(
              `[Socket.IO] Received leaderboard: ${data.users ? data.users.length : 0} users`
            );
            resolve(data);
          } else {
            reject(new Error("Empty leaderboard response"));
          }
          socket.disconnect();
          socket = null;
        });
      });

      socket.on("connect_error", (err) => {
        clearTimeout(timeout);
        console.error("[Socket.IO] Connection error:", err.message);
        reject(err);
        if (socket) {
          socket.disconnect();
          socket = null;
        }
      });

      socket.on("error", (err) => {
        clearTimeout(timeout);
        console.error("[Socket.IO] Error:", err);
        reject(err);
      });
    } catch (err) {
      clearTimeout(timeout);
      reject(err);
    }
  });
}

/**
 * Fetch a player's most recent replay (game) via REST API.
 * Returns the timestamp of the last game, or null if no games found.
 */
async function fetchPlayerLastGame(username) {
  try {
    const url = `${GENERALS_API_BASE}/replaysForUsername?u=${encodeURIComponent(
      username
    )}&offset=0&count=1`;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (!response.ok) {
      console.error(`[API] HTTP ${response.status} for user: ${username}`);
      return null;
    }

    const replays = await response.json();

    if (Array.isArray(replays) && replays.length > 0) {
      const lastReplay = replays[0];
      return {
        started: lastReplay.started, // timestamp in ms
        replayId: lastReplay.id,
        type: lastReplay.type,
        ladderId: lastReplay.ladder_id,
        turns: lastReplay.turns,
        ranking: lastReplay.ranking,
      };
    }
    return null;
  } catch (err) {
    if (err.name === "AbortError") {
      console.error(`[API] Timeout for user: ${username}`);
    } else {
      console.error(`[API] Error fetching replays for ${username}:`, err.message);
    }
    return null;
  }
}

/**
 * Fetch a player's current stars and rank via REST API.
 */
async function fetchPlayerStarsAndRanks(username) {
  try {
    const url = `${GENERALS_API_BASE}/starsAndRanks?u=${encodeURIComponent(username)}`;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (!response.ok) return null;

    const data = await response.json();
    return data;
  } catch (err) {
    console.error(`[API] Error fetching stars for ${username}:`, err.message);
    return null;
  }
}

/**
 * Fetch all data: leaderboard + each player's last game time.
 * Processes players in batches to avoid rate limiting.
 */
async function fetchAllData() {
  if (isFetching) {
    console.log("[Data] Already fetching, skipping...");
    return;
  }

  isFetching = true;
  console.log("[Data] Starting data fetch...");

  try {
    // Step 1: Get leaderboard via Socket.IO
    const leaderboardData = await fetchLeaderboardViaSocket();

    if (!leaderboardData || !leaderboardData.users) {
      throw new Error("Invalid leaderboard data");
    }

    const users = leaderboardData.users.slice(0, PLAYER_COUNT);
    const stars = leaderboardData.stars || [];
    const supporters = leaderboardData.supporters || [];

    console.log(`[Data] Processing ${users.length} players...`);

    // Step 2: Fetch last game for each player (in batches of 10)
    const BATCH_SIZE = 10;
    const players = [];

    for (let i = 0; i < users.length; i += BATCH_SIZE) {
      const batch = users.slice(i, i + BATCH_SIZE);
      const batchResults = await Promise.all(
        batch.map(async (username, batchIdx) => {
          const globalIdx = i + batchIdx;
          const lastGame = await fetchPlayerLastGame(username);

          return {
            rank: globalIdx + 1,
            username: username,
            stars: parseFloat(stars[globalIdx]) || 0,
            isSupporter: supporters[globalIdx] || false,
            lastGame: lastGame
              ? {
                  started: lastGame.started,
                  timeAgoMs: Date.now() - lastGame.started,
                  replayId: lastGame.replayId,
                  turns: lastGame.turns,
                  type: lastGame.type,
                }
              : null,
            replayUrl: lastGame
              ? `https://generals.io/replays/${lastGame.replayId}?p=${encodeURIComponent(username)}`
              : null,
          };
        })
      );

      players.push(...batchResults);
      console.log(
        `[Data] Processed batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(users.length / BATCH_SIZE)}`
      );

      // Small delay between batches
      if (i + BATCH_SIZE < users.length) {
        await new Promise((r) => setTimeout(r, 500));
      }
    }

    cachedLeaderboard = {
      players: players,
      updatedAt: Date.now(),
      totalPlayers: users.length,
    };
    lastUpdateTime = Date.now();

    console.log(`[Data] Fetch complete. ${players.length} players cached.`);
  } catch (err) {
    console.error("[Data] Fetch error:", err.message);
    // Keep using cached data if available
    if (!cachedLeaderboard) {
      cachedLeaderboard = {
        players: [],
        updatedAt: 0,
        totalPlayers: 0,
        error: err.message,
      };
    }
  } finally {
    isFetching = false;
  }
}

// ========== REST API Endpoints ==========

app.get("/api/leaderboard", (req, res) => {
  if (!cachedLeaderboard) {
    return res.json({
      players: [],
      updatedAt: 0,
      message: "Data not yet loaded. Please wait...",
    });
  }
  res.json(cachedLeaderboard);
});

app.get("/api/status", (req, res) => {
  res.json({
    isFetching,
    lastUpdate: lastUpdateTime,
    hasData: !!cachedLeaderboard,
    playerCount: cachedLeaderboard ? cachedLeaderboard.players.length : 0,
  });
});

// ========== Server Startup ==========

server.listen(PORT, () => {
  console.log(`[Server] Running on http://localhost:${PORT}`);
  console.log("[Server] Starting initial data fetch...");

  // Initial fetch
  fetchAllData().then(() => {
    console.log("[Server] Initial data loaded!");
  });

  // Auto-refresh every 2 minutes
  setInterval(() => {
    fetchAllData();
  }, 2 * 60 * 1000);
});
