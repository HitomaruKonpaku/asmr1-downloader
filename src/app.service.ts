import { Injectable, MessageEvent } from '@nestjs/common';
import Bottleneck from 'bottleneck';
import { createWriteStream, existsSync, mkdirSync, statSync } from 'fs';
import { join } from 'path';
import { Observable, Subscriber } from 'rxjs';
import { pipeline } from 'stream/promises';

interface TrackNode {
  type: string;
  title: string;
  children?: TrackNode[];
  mediaDownloadUrl?: string;
  size?: number;
}

interface FileEntry {
  path: string;
  url: string;
  size?: number;
}

const API_BASE = process.env.API_BASE || 'https://api.asmr-200.com/api/tracks';
const DOWNLOAD_DIR = process.env.DOWNLOAD_DIR || join(process.cwd(), 'download');
const DOWNLOAD_PARALLEL = Number(process.env.DOWNLOAD_PARALLEL) || 2;
const DOWNLOAD_TIMEOUT = Number(process.env.DOWNLOAD_TIMEOUT) || 120_000;

@Injectable()
export class AppService {
  private readonly limiter = new Bottleneck({ maxConcurrent: DOWNLOAD_PARALLEL });
  private readonly activeIds = new Set<string>();

  download(workId: string): Observable<MessageEvent> {
    return new Observable((subscriber: Subscriber<MessageEvent>) => {
      const numericId: string = workId.replace(/^RJ/i, '');
      const normalizedId: string = `RJ${numericId}`;

      if (this.activeIds.has(normalizedId)) {
        this.log(normalizedId, 'rejected: already in progress');
        this.emit(subscriber, 'error', `${normalizedId} is already in progress`);
        subscriber.complete();
        return;
      }

      this.log(normalizedId, 'queued');
      this.activeIds.add(normalizedId);
      this.run(workId, subscriber)
        .catch((err) => {
          this.log(normalizedId, `failed: ${err.message || 'Unknown error'}`);
          this.emit(subscriber, 'error', err.message || 'Unknown error');
          subscriber.complete();
        })
        .finally(() => {
          this.log(normalizedId, 'finished');
          this.activeIds.delete(normalizedId);
        });
    });
  }

  private async run(
    rawId: string,
    subscriber: Subscriber<MessageEvent>,
  ): Promise<void> {
    const numericId: string = rawId.replace(/^RJ/i, '');
    const workId: string = `RJ${numericId}`;

    this.log(workId, 'fetching metadata');
    this.emit(subscriber, 'log', `Fetching data for ${workId}...`);

    let json: any;
    try {
      const res = await fetch(`${API_BASE}/${numericId}?v=2`);
      json = await res.json();
      this.log(workId, `metadata fetched: HTTP ${res.status}`);
      this.emit(subscriber, 'log', `API fetch success (HTTP ${res.status})`);
    } catch (err: any) {
      this.log(workId, `metadata fetch failed: ${err.message}`);
      this.emit(subscriber, 'error', `API fetch failed: ${err.message}`);
      subscriber.complete();
      return;
    }

    if (!Array.isArray(json)) {
      const errorMsg: string = json?.error || 'Invalid API response';
      this.emit(subscriber, 'error', errorMsg);
      subscriber.complete();
      return;
    }

    const files: FileEntry[] = [];
    this.flattenTree(json, '', files);

    this.emit(subscriber, 'log', `Found ${files.length} files. Starting download...`);
    this.log(workId, `starting download of ${files.length} files`);

    const destDir: string = join(DOWNLOAD_DIR, workId);
    let completed: number = 0;
    const total: number = files.length;

    const downloadOne = async (entry: FileEntry): Promise<void> => {
      const dest: string = join(destDir, entry.path);
      const dir: string = join(dest, '..');
      mkdirSync(dir, { recursive: true });

      if (existsSync(dest)) {
        const localSize: number = statSync(dest).size;
        if (entry.size === undefined || localSize === entry.size) {
          this.log(workId, `skipped: ${entry.path}`);
          completed++;
          this.emit(
            subscriber,
            'progress',
            JSON.stringify({
              file: entry.path,
              status: 'skipped',
              completed,
              total,
            }),
          );
          return;
        }
      }

      try {
        const resp = await fetch(entry.url, { signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT) });
        if (!resp.ok || !resp.body) {
          throw new Error(`HTTP ${resp.status}`);
        }
        const ws = createWriteStream(dest);
        await pipeline(resp.body as any, ws);
        completed++;
        this.emit(
          subscriber,
          'progress',
          JSON.stringify({
            file: entry.path,
            status: 'done',
            completed,
            total,
          }),
        );
      } catch (err: any) {
        this.log(workId, `file failed: ${entry.path} - ${err.message}`);
        completed++;
        this.emit(
          subscriber,
          'progress',
          JSON.stringify({
            file: entry.path,
            status: 'failed',
            error: err.message,
            completed,
            total,
          }),
        );
      }
    };

    // Download with shared concurrency limiter
    await Promise.all(
      files.map((entry) => this.limiter.schedule(() => downloadOne(entry))),
    );

    this.log(workId, `completed: ${total} files processed`);
    this.emit(subscriber, 'done', `Downloaded ${total} files to ${workId}/`);
    subscriber.complete();
  }

  private flattenTree(
    nodes: TrackNode[],
    prefix: string,
    result: FileEntry[],
  ): void {
    for (const node of nodes) {
      if (node.type === 'folder' && node.children) {
        this.flattenTree(node.children, `${prefix}/${node.title}`, result);
      } else if (node.mediaDownloadUrl) {
        result.push({
          path: `${prefix}/${node.title}`,
          url: node.mediaDownloadUrl,
          size: node.size,
        });
      }
    }
  }

  private emit(
    subscriber: Subscriber<MessageEvent>,
    type: string,
    data: string,
  ): void {
    subscriber.next({ type, data } as MessageEvent);
  }

  private log(workId: string, message: string): void {
    console.log(`[ASMR] [${workId}] ${message}`);
  }
}
