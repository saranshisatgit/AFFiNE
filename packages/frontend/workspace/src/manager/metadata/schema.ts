import { WorkspaceFlavour } from '@affine/env/workspace';
import { WorkspaceVersion } from '@toeverything/infra/blocksuite';
import { z } from 'zod';

const workspaceMetadataSchema = z.object({
  id: z.string(),
  flavour: z.nativeEnum(WorkspaceFlavour),
  version: z.optional(z.nativeEnum(WorkspaceVersion)),
});

export const workspaceMetadataArraySchema = z.array(workspaceMetadataSchema);

export type WorkspaceMetadata = z.infer<typeof workspaceMetadataSchema>;
