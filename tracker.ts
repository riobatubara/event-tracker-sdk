interface AnalyticsEvent {
  event: string;
  properties: Record<string, any>;
  timestamp: number;
}

interface SdkConfig {
  endpoint: string;
  batchSize?: number;
  flushIntervalMs?: number;
  maxRetryAttempts?: number;
  initialRetryDelayMs?: number;
  trackPageViews?: boolean; // NEW: Toggle automatic page view tracking on/off
}

export class EventTracker {
  private endpoint: string;
  private batchSize: number;
  private flushIntervalMs: number;
  private maxRetryAttempts: number;
  private initialRetryDelayMs: number;

  private buffer: AnalyticsEvent[] = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private isSending = false;
  private readonly storageKey = 'tx_events_retry_queue';
  private lastTrackedUrl = ''; // NEW: Cache to prevent duplicate event tracking

  constructor(config: SdkConfig) {
    this.endpoint = config.endpoint;
    this.batchSize = config.batchSize ?? 10;
    this.flushIntervalMs = config.flushIntervalMs ?? 5000;
    this.maxRetryAttempts = config.maxRetryAttempts ?? 5;
    this.initialRetryDelayMs = config.initialRetryDelayMs ?? 1000;

    this.initHeartbeat();
    this.initOfflineListeners();

    // NEW: If enabled, start watching the browser URL automatically
    if (config.trackPageViews ?? true) {
      this.initPageViewTracking();
    }
  }

  // Public API to track events
  public track(eventName: string, customProperties: Record<string, any> = {}): void {
    const event: AnalyticsEvent = {
      event: eventName,
      properties: {
        ...this.getContextEnrichment(),
        ...customProperties,
      },
      timestamp: Date.now(),
    };

    this.buffer.push(event);

    if (this.buffer.length >= this.batchSize) {
      this.flush();
    }
  }

  // NEW METHOD: Monitors URL route updates dynamically
  private initPageViewTracking(): void {
    if (typeof window === 'undefined') return;

    // 1. Track the very first initial page load landing
    this.trackPageView();

    // 2. Listen for standard browser Back/Forward clicks
    window.addEventListener('popstate', () => this.trackPageView());

    // 3. Monkey-patch pushState (Triggers when single-page apps update the URL quietly)
    const originalPushState = window.history.pushState;
    const trackerInstance = this; // Capture the outer context safely
    
    window.history.pushState = function (...args) {
      originalPushState.apply(this, args); // Execute original behavior first
      trackerInstance.trackPageView(); // Trigger our tracker immediately right after
    };

    // 4. Monkey-patch replaceState (Triggers when single-page apps redirect paths quietly)
    const originalReplaceState = window.history.replaceState;
    window.history.replaceState = function (...args) {
      originalReplaceState.apply(this, args);
      trackerInstance.trackPageView();
    };
  }

  // Helper method to execute a filtered Page View track
  private trackPageView(): void {
    const currentUrl = window.location.href;
    
    // Prevent double-tracking the exact same page path continuously
    if (currentUrl === this.lastTrackedUrl) return;
    
    this.lastTrackedUrl = currentUrl;
    this.track('page_view', {
      title: document.title
    });
  }

  // Force send all events currently in buffer
  public async flush(): Promise<void> {
    if (this.buffer.length === 0 || this.isSending) return;

    const payload = [...this.buffer];
    this.buffer = [];
    this.isSending = true;

    try {
      await this.sendRequestWithBackoff(payload);
    } catch (error) {
      this.saveToStorage(payload);
    } finally {
      this.isSending = false;
    }
  }

  // Gather system details automatically
  private getContextEnrichment(): Record<string, any> {
    if (typeof window === 'undefined') return {};

    return {
      url: window.location.href,
      referrer: document.referrer,
      screen_resolution: `${window.screen.width}x${window.screen.height}`,
      viewport_size: `${window.innerWidth}x${window.innerHeight}`,
      user_agent: navigator.userAgent,
      language: navigator.language,
    };
  }

  // Network request worker with Exponential Backoff
  private async sendRequestWithBackoff(events: AnalyticsEvent[]): Promise<void> {
    let attempt = 0;

    while (attempt < this.maxRetryAttempts) {
      try {
        if (!navigator.onLine) {
          throw new Error('Network offline');
        }

        const response = await fetch(this.endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ events }),
        });

        if (response.ok) {
          return; 
        }

        if (response.status >= 400 && response.status < 500) {
          throw new Error(`Client Error: ${response.status}`);
        }

        throw new Error(`Server Error: ${response.status}`);

      } catch (error) {
        attempt++;
        if (attempt >= this.maxRetryAttempts) {
          throw error;
        }

        const delay = this.initialRetryDelayMs * Math.pow(2, attempt - 1);
        console.warn(`⚠️ Request failed. Retrying attempt ${attempt}/${this.maxRetryAttempts} in ${delay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  // Set up the time-based flush interval
  private initHeartbeat(): void {
    this.timer = setInterval(() => this.flush(), this.flushIntervalMs);
  }

  // Listen for connection recovery
  private initOfflineListeners(): void {
    if (typeof window === 'undefined') return;

    window.addEventListener('online', () => this.retryStoredEvents());
    this.retryStoredEvents();
  }

  // Persistence management
  private saveToStorage(events: AnalyticsEvent[]): void {
    try {
      const existing = this.getStoredEvents();
      const updated = [...existing, ...events];
      localStorage.setItem(this.storageKey, JSON.stringify(updated));
    } catch (e) {
      console.error('Failed to save events to localStorage', e);
    }
  }

  private getStoredEvents(): AnalyticsEvent[] {
    try {
      const data = localStorage.getItem(this.storageKey);
      return data ? JSON.parse(data) : [];
    } catch {
      return [];
    }
  }

  private async retryStoredEvents(): Promise<void> {
    if (this.isSending) return;
    
    const stored = this.getStoredEvents();
    if (stored.length === 0) return;

    this.isSending = true;
    localStorage.removeItem(this.storageKey);

    for (let i = 0; i < stored.length; i += this.batchSize) {
      const chunk = stored.slice(i, i + this.batchSize);
      try {
        await this.sendRequestWithBackoff(chunk);
      } catch (error) {
        const failedRemaining = stored.slice(i);
        this.saveToStorage(failedRemaining);
        break;
      }
    }
    this.isSending = false;
  }

  public destroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.flush();
  }
}
