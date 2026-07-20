interface AnalyticsEvent {
  event: string;
  properties: Record<string, any>;
  timestamp: number;
}

interface SdkConfig {
  endpoint: string;
  batchSize?: number;
  flushIntervalMs?: number;
}

export class EventTracker {
  private endpoint: string;
  private batchSize: number;
  private flushIntervalMs: number;
  private buffer: AnalyticsEvent[] = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private isSending = false;
  private readonly storageKey = 'tx_events_retry_queue';

  constructor(config: SdkConfig) {
    this.endpoint = config.endpoint;
    this.batchSize = config.batchSize ?? 10;
    this.flushIntervalMs = config.flushIntervalMs ?? 5000;

    this.initHeartbeat();
    this.initOfflineListeners();
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

  // Force send all events currently in buffer
  public async flush(): Promise<void> {
    if (this.buffer.length === 0 || this.isSending) return;

    const payload = [...this.buffer];
    this.buffer = [];
    this.isSending = true;

    try {
      await this.sendRequest(payload);
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

  // Network request handler
  private async sendRequest(events: AnalyticsEvent[]): Promise<void> {
    if (!navigator.onLine) {
      throw new Error('Network unavailable');
    }

    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    });

    if (!response.ok) {
      throw new Error(`Server returned status ${response.status}`);
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
    // Run an initial check on startup in case offline data exists
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

    // Chunk the stored items back into regular batch sizes for safety
    for (let i = 0; i < stored.length; i += this.batchSize) {
      const chunk = stored.slice(i, i + this.batchSize);
      try {
        await this.sendRequest(chunk);
      } catch (error) {
        // Put remaining un-sent items back if network fails mid-way
        const failedRemaining = stored.slice(i);
        this.saveToStorage(failedRemaining);
        break;
      }
    }
    this.isSending = false;
  }

  // Cleanup system resources
  public destroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.flush();
  }
}
