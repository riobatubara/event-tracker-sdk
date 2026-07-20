import { EventTracker } from './tracker';

// 1. Initialize your new SDK
const tracker = new EventTracker({
  endpoint: 'https://example.com',
  batchSize: 3, // Set low to '3' so we can test it easily!
  flushIntervalMs: 5000
});

console.log("🚀 Tracker initialized!");

// 2. Track some events
tracker.track('button_click', { button_color: 'blue' });
tracker.track('page_view', { previous_page: 'home' });

console.log("📦 2 events added to memory queue...");

// 3. This 3rd event will trigger the batch size limit and attempt to send!
tracker.track('purchase', { item: 'coffee' }); 
console.log("⚡ 3rd event added! Queue flushed automatically.");
