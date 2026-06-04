import { Controller, MessageEvent, Query, Sse } from '@nestjs/common';
import { Observable } from 'rxjs';
import { AppService } from './app.service';

@Controller('api')
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Sse('download')
  download(@Query('id') workId: string): Observable<MessageEvent> {
    return this.appService.download(workId);
  }
}
