# event-tracker-sdk

A lightweight, high-performance TypeScript Event Tracker SDK that handles client-side analytics with queueing, batch processing, context enrichment, and offline resilience.

## Features
* **Queueing & Batching**: Buffers events in memory. Flushes automatically when reaching a threshold (e.g., 3 events) or every 5 seconds.
* **Context Enrichment**: Automatically captures metadata like page URL, screen resolution, viewport size, and user agent.
* **Offline Support & Persistence**: Automatically falls back to `localStorage` if the network fails or goes offline, and auto-retries when the connection restores.

---

## Requirements
Make sure you have the following installed on your machine:
* [Node.js](https://nodejs.org) (Version 18 or newer recommended)

---

## Setup

### 1. Install Dependencies
Clone or create your project directory, navigate into it, and install the required development tools:
```bash
npm install
```
*(If setting up from scratch, run `npm install -D typescript tsx @types/node http-server`)*

---

## How to Run the Tests

### Option 1: Quick Terminal Run (Memory Test)
You can execute the code instantly inside your terminal using `tsx` to ensure your TypeScript syntax, queue counts, and logic loops are working flawlessly.

Run this command:
```bash
npx tsx index.ts
```

**What to expect:**
* You will see the tracker initialize and stack 2 items in memory.
* On the 3rd event item, it will attempt an automatic flush.
* *Note: Because terminals do not possess browser environments, you will notice a standard `localStorage is not defined` notice here—this proves your code successfully caught the network boundary error as intended!*

---

### Option 2: Live Browser Run (Full Feature Test)
To watch the SDK capture screen sizes, save to real local storage, and stream actual button clicks, deploy it directly to a local browser runtime.

#### Step A: Compile TypeScript into Browser-ready JavaScript
Web browsers cannot parse `.ts` files natively. Run this compilation script to generate a standard browser-compatible `tracker.js` file:
```bash
npx tsc tracker.ts --target es2022 --module es2020
```

#### Step B: Launch the Local Test Server
Run a tiny network server to host your local workspace safely without triggering strict local file security blocks (`CORS` errors):
```bash
npx http-server .
```

#### Step C: Interact and Observe
1. Open the local address provided by your terminal in your browser (usually `http://localhost:8080`).
2. **Right-click** anywhere on the webpage and select **Inspect / Inspect Element**, then navigate directly to the **Console** tab.
3. Click the interactive blue and green buttons on the page.
4. Notice how the first two clicks buffer safely. On your **third click**, the system automatically batches all accumulated actions into a single network payload!

---

## Testing Offline Support
Want to watch the persistence engine save data safely through internet drops?
1. Open your browser's Developer Tools (**Inspect**).
2. Go to the **Network** tab and toggle the connection dropdown from **No Throttling** to **Offline**.
3. Go back to the page and click the buttons 3 times to trigger a batch send.
4. Look under the **Application** (or **Storage**) tab -> **Local Storage**. You will see your exact custom actions preserved inside the `tx_events_retry_queue` payload, waiting patiently to be sent!
5. Toggle your network back to **No Throttling** (Online). Your code instantly detects the active connection and clears out the offline queue safely.
Use code with caution.If you are ready to expand the codebase, tell me if you want to add custom configuration parameters (like letting users change the localStorage key name) or learn how to pack this project so others can install it via npm.2 sites