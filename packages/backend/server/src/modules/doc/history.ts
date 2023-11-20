import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { Snapshot, SnapshotHistory } from '@prisma/client';

import { PrismaService } from '../../prisma';
import { SubscriptionStatus } from '../payment/service';
import { Permission } from '../workspaces/types';

@Injectable()
export class DocHistoryManager {
  constructor(private readonly db: PrismaService) {}

  @OnEvent('doc:manager:snapshot:beforeUpdate')
  async onDocUpdated(snapshot: Snapshot) {
    const last = await this.lastHistory(snapshot.workspaceId, snapshot.id);
    const data: SnapshotHistory = {
      workspaceId: snapshot.workspaceId,
      id: snapshot.id,
      seq: snapshot.seq,
      blob: snapshot.blob,
      state: snapshot.state,
      createdAt: snapshot.createdAt,
      expiredAt: await this.getExpiredDateFromNow(snapshot.workspaceId),
    };

    if (
      !last ||
      last.createdAt.getTime() <
        snapshot.updatedAt.getTime() - 1000 * 60 * 10 /* 10 mins */
    ) {
      // create new snapshot history record if never created or last created was before 10 mins ago
      await this.db.snapshotHistory.create({
        select: {
          createdAt: true,
        },
        data,
      });
    } else if (last && last.createdAt !== snapshot.updatedAt) {
      // replace the last snapshot history
      await this.db.snapshotHistory.update({
        select: {
          createdAt: true,
        },
        where: {
          workspaceId_id_seq: {
            workspaceId: snapshot.workspaceId,
            id: snapshot.id,
            seq: last.seq,
          },
        },
        data,
      });
    }
  }

  async list(workspaceId: string, id: string) {
    return this.db.snapshotHistory.findMany({
      select: {
        seq: true,
        createdAt: true,
      },
      where: {
        workspaceId,
        id,
        // only include the ones has not expired
        expiredAt: {
          gt: new Date(),
        },
      },
    });
  }

  async get(workspaceId: string, id: string, seq: number) {
    return this.db.snapshotHistory.findUnique({
      where: {
        workspaceId_id_seq: {
          workspaceId,
          id,
          seq,
        },
      },
    });
  }

  async recover(workspaceId: string, id: string, seq: number) {
    const history = await this.db.snapshotHistory.findUnique({
      where: {
        workspaceId_id_seq: {
          workspaceId,
          id,
          seq,
        },
      },
    });

    if (!history) {
      throw new Error('Given history not found');
    }

    const oldSnapshot = await this.db.snapshot.findUnique({
      where: {
        id_workspaceId: {
          id,
          workspaceId,
        },
      },
    });

    if (!oldSnapshot) {
      // unreachable actually
      throw new Error('Given snapshot not found');
    }

    // save old snapshot as one history record
    await this.onDocUpdated(oldSnapshot);
    await this.db.snapshot.update({
      where: {
        id_workspaceId: {
          id,
          workspaceId,
        },
      },
      data: {
        blob: history.blob,
        state: history.state,
      },
    });

    return history;
  }

  async lastHistory(workspaceId: string, id: string) {
    return this.db.snapshotHistory.findFirst({
      where: {
        workspaceId,
        id,
      },
      select: {
        seq: true,
        createdAt: true,
      },
      orderBy: {
        createdAt: 'desc',
      },
    });
  }

  /**
   * @todo(@darkskygit) refactor with [Usage Control] system
   */
  async getExpiredDateFromNow(workspaceId: string) {
    const permission = await this.db.workspaceUserPermission.findFirst({
      select: {
        userId: true,
      },
      where: {
        workspaceId,
        type: Permission.Owner,
      },
    });

    if (!permission) {
      // unreachable actually
      throw new Error('Workspace owner not found');
    }

    const sub = await this.db.userSubscription.findFirst({
      select: {
        id: true,
      },
      where: {
        userId: permission.userId,
        status: SubscriptionStatus.Active,
      },
    });

    return new Date(
      Date.now() +
        1000 *
          60 *
          60 *
          24 *
          // 30 days for subscription user, 7 days for free user
          (sub ? 30 : 7)
    );
  }
}
