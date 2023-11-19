import type { Workspace } from '@blocksuite/store';

import type { WorkspaceMetadata } from '../metadata';

export interface WorkspaceSource {
  list(): Promise<WorkspaceMetadata[]>;
  create(copyFrom: Workspace): Promise<WorkspaceMetadata>;
  delete(id: string): Promise<void>;
}
