interface AnalyticsEvent {
  event: string;
  properties: Record<string, any>;
  timestamp: number;
}

interface SdkConfig {
  endpoint: string;
  batchSize?: number;
  flushIntervalMs?: number;
  maxRetryAttempts?: number; // Maximum times to retry before giving up
  initialRetryDelayMs?: number; // Starting delay (e.g., 1 second)
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

  constructor(config: SdkConfig) {
    this.endpoint = config.endpoint;
    this.batchSize = config.batchSize ?? 10;
    this.flushIntervalMs = config.flushIntervalMs ?? 5000;
    this.maxRetryAttempts = config.maxRetryAttempts ?? 5;
    this.initialRetryDelayMs = config.initialRetryDelayMs ?? 1000;

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
      await this.sendRequestWithBackoff(payload);
    } catch (error) {
      // If backoff completely fails (or network goes dead), dump safely to localStorage
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

        // 2xx response status codes mean total success
        if (response.ok) {
          return; 
        }

        // If server gives a client-side error (like 400 Bad Request or 403 Forbidden), 
        // retrying won't change the outcome. Break early and throw to prevent infinite loops.
        if (response.status >= 400 && response.status < 500) {
          throw new Error(`Client Error: ${response.status}`);
        }

        // If it reaches here, it's likely a 5xx Server Error. Loop continues to try again!
        throw new Error(`Server Error: ${response.status}`);

      } catch (error) {
        attempt++;
        
        // If we have exhausted our maximum allowed retries, pass the error up to the handler
        if (attempt >= this.maxRetryAttempts) {
          throw error;
        }

        // Calculate Exponential Backoff Delay: Delay = InitialDelay * 2^(attempt - 1)
        // Attempt 1 delay: 1000 * 2^0 = 1000ms (1s)
        // Attempt 2 delay: 1000 * 2^1 = 2000ms (2s)
        // Attempt 3 delay: 1000 * 2^2 = 4000ms (4s)
        const delay = this.initialRetryDelayMs * Math.pow(2, attempt - 1);
        
        console.warn(`⚠️ Request failed. Retrying attempt ${attempt}/${this.maxRetryAttempts} in ${delay}ms...`);
        
        // Wait for the calculated delay duration before moving to the next iteration loop
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

    // Chunk the stored items back into regular batch sizes for safety
    for (let i = 0; i < stored.length; i += this.batchSize) {
      const chunk = stored.slice(i, i + this.batchSize);
      try {
        await this.sendRequestWithBackoff(chunk);
      } catch (error) {
        // Put remaining un-sent items back if network or backoff fails mid-way
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
