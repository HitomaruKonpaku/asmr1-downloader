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
const DOWNLOAD_PARALLEL = Number(process.env.DOWNLOAD_PARALLEL) || 2;
const DOWNLOAD_DIR = process.env.DOWNLOAD_DIR || join(process.cwd(), 'download');

@Injectable()
export class AppService {
  download(workId: string): Observable<MessageEvent> {
    return new Observable((subscriber: Subscriber<MessageEvent>) => {
      this.run(workId, subscriber).catch((err) => {
        this.emit(subscriber, 'error', err.message || 'Unknown error');
        subscriber.complete();
      });
    });
  }

  private async run(
    rawId: string,
    subscriber: Subscriber<MessageEvent>,
  ): Promise<void> {
    const numericId: string = rawId.replace(/^RJ/i, '');
    const workId: string = `RJ${numericId}`;

    this.emit(subscriber, 'log', `Fetching data for ${workId}...`);

    let json: any;
    try {
      const res = await fetch(`${API_BASE}/${numericId}?v=2`);
      json = await res.json();
      this.emit(subscriber, 'log', `API fetch success (HTTP ${res.status})`);
    } catch (err: any) {
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
        const resp = await fetch(entry.url);
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

    // Download with concurrency limiter
    const limiter = new Bottleneck({ maxConcurrent: DOWNLOAD_PARALLEL });
    await Promise.all(
      files.map((entry) => limiter.schedule(() => downloadOne(entry))),
    );

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
}
