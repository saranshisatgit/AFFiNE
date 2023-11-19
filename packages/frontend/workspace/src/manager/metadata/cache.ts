import type { z } from 'zod';

import { workspaceMetadataArraySchema } from './schema';

const METADATA_STORAGE_KEY = 'jotai-workspaces';

export function readWorkspaceMetadataFromLocalStorage() {
  // don't change this key,
  // otherwise it will cause the data loss in the production
  const primitiveMetadata = localStorage.getItem(METADATA_STORAGE_KEY);
  if (primitiveMetadata) {
    try {
      const items = JSON.parse(primitiveMetadata) as z.infer<
        typeof workspaceMetadataArraySchema
      >;
      workspaceMetadataArraySchema.parse(items);
      return [...items];
    } catch (e) {
      console.error('cannot parse worksapce', e);
    }
    return [];
  }
  return [];
}

export function writeWorkspaceMetadataToLocalStorage(
  metadata: z.infer<typeof workspaceMetadataArraySchema>
) {
  const metadataMap = new Map(metadata.map(x => [x.id, x]));
  metadata = Array.from(metadataMap.values());
  // write back to localStorage
  workspaceMetadataArraySchema.parse(metadata);
  localStorage.setItem(METADATA_STORAGE_KEY, JSON.stringify(metadata));
}
