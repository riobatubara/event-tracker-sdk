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
  trackPageViews?: boolean;
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
  private lastTrackedUrl = '';
  
  // NEW: In-memory array fallback if localStorage is missing or blocked
  private memoryStorageFallback: string = '[]'; 

  constructor(config: SdkConfig) {
    this.endpoint = config.endpoint;
    this.batchSize = config.batchSize ?? 10;
    this.flushIntervalMs = config.flushIntervalMs ?? 5000;
    this.maxRetryAttempts = config.maxRetryAttempts ?? 5;
    this.initialRetryDelayMs = config.initialRetryDelayMs ?? 1000;

    // SAFE ENVIRONMENT CHECK: Only initialize timers and listeners on the client side
    if (typeof window !== 'undefined') {
      this.initHeartbeat();
      this.initOfflineListeners();

      if (config.trackPageViews ?? true) {
        this.initPageViewTracking();
      }
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

  // Monitor URL route updates dynamically
  private initPageViewTracking(): void {
    if (typeof window === 'undefined') return;

    this.trackPageView();

    window.addEventListener('popstate', () => this.trackPageView());

    const originalPushState = window.history.pushState;
    const trackerInstance = this;
    
    window.history.pushState = function (...args) {
      originalPushState.apply(this, args);
      trackerInstance.trackPageView();
    };

    const originalReplaceState = window.history.replaceState;
    window.history.replaceState = function (...args) {
      originalReplaceState.apply(this, args);
      trackerInstance.trackPageView();
    };
  }

  private trackPageView(): void {
    if (typeof window === 'undefined') return;
    
    const currentUrl = window.location.href;
    if (currentUrl === this.lastTrackedUrl) return;
    
    this.lastTrackedUrl = currentUrl;
    this.track('page_view', {
      title: typeof document !== 'undefined' ? document.title : ''
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

  // Gather system details automatically (Heavily fortified against Server Environments)
  private getContextEnrichment(): Record<string, any> {
    // Return empty context details if running on Node.js / Server Side
    if (typeof window === 'undefined') {
      return { environment: 'server' }; 
    }

    return {
      environment: 'browser',
      url: window.location.href,
      referrer: typeof document !== 'undefined' ? document.referrer : '',
      screen_resolution: window.screen ? `${window.screen.width}x${window.screen.height}` : '',
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
        // Safe check for navigator availability
        if (typeof navigator !== 'undefined' && !navigator.onLine) {
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
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  private initHeartbeat(): void {
    this.timer = setInterval(() => this.flush(), this.flushIntervalMs);
  }

  private initOfflineListeners(): void {
    if (typeof window === 'undefined') return;

    window.addEventListener('online', () => this.retryStoredEvents());
    this.retryStoredEvents();
  }

  // NEW ENHANCEMENT: Fail-safe Storage Writer
  private saveToStorage(events: AnalyticsEvent[]): void {
    try {
      const existing = this.getStoredEvents();
      const updated = [...existing, ...events];
      const serialized = JSON.stringify(updated);

      if (this.isLocalStorageAvailable()) {
        localStorage.setItem(this.storageKey, serialized);
      } else {
        this.memoryStorageFallback = serialized;
      }
    } catch (e) {
      console.error('Failed to preserve events safely', e);
    }
  }

  // NEW ENHANCEMENT: Fail-safe Storage Reader
  private getStoredEvents(): AnalyticsEvent[] {
    try {
      let data: string | null = null;

      if (this.isLocalStorageAvailable()) {
        data = localStorage.getItem(this.storageKey);
      } else {
        data = this.memoryStorageFallback;
      }

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

    if (this.isLocalStorageAvailable()) {
      localStorage.removeItem(this.storageKey);
    } else {
      this.memoryStorageFallback = '[]';
    }

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

  // NEW ENHANCEMENT: Robust capability check helper
  // Catches edge-cases where localStorage exists but is explicitly blocked by Safari Incognito or Chrome Privacy modes
  private isLocalStorageAvailable(): boolean {
    if (typeof window === 'undefined' || !('localStorage' in window)) {
      return false;
    }
    try {
      const testKey = '__storage_test__';
      localStorage.setItem(testKey, testKey);
      localStorage.removeItem(testKey);
      return true;
    } catch (e) {
      return false;
    }
  }

  public destroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.flush();
  }
}
