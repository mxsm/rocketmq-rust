export interface NameserverDraft {
  namesrvAddrList: string[];
  currentNamesrv: string | null;
}

export function normalizeNameserverDraft(draft: NameserverDraft): NameserverDraft {
  const namesrvAddrList = [...new Set(draft.namesrvAddrList.map((address) => address.trim()).filter(Boolean))];
  const requestedCurrent = draft.currentNamesrv?.trim() ?? '';
  const currentNamesrv = namesrvAddrList.includes(requestedCurrent) ? requestedCurrent : namesrvAddrList[0] ?? null;

  return { namesrvAddrList, currentNamesrv };
}

export function isNameserverDraftDirty(draft: NameserverDraft, saved: NameserverDraft) {
  const normalizedDraft = normalizeNameserverDraft(draft);
  const normalizedSaved = normalizeNameserverDraft(saved);

  return normalizedDraft.currentNamesrv !== normalizedSaved.currentNamesrv
    || normalizedDraft.namesrvAddrList.length !== normalizedSaved.namesrvAddrList.length
    || normalizedDraft.namesrvAddrList.some((address, index) => address !== normalizedSaved.namesrvAddrList[index]);
}
