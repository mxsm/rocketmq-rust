import { isNameserverDraftDirty, normalizeNameserverDraft } from './settings-model';

describe('normalizeNameserverDraft', () => {
  it('trims, removes duplicate and empty addresses, and selects the first valid address when current is invalid', () => {
    expect(normalizeNameserverDraft({
      namesrvAddrList: [' 10.0.0.10:9876 ', '', '10.0.0.11:9876', '10.0.0.10:9876'],
      currentNamesrv: 'missing:9876'
    })).toEqual({
      namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
      currentNamesrv: '10.0.0.10:9876'
    });
  });

  it('retains a current NameServer after normalizing its value', () => {
    expect(normalizeNameserverDraft({
      namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
      currentNamesrv: ' 10.0.0.11:9876 '
    }).currentNamesrv).toBe('10.0.0.11:9876');
  });
});

describe('isNameserverDraftDirty', () => {
  const saved = {
    namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
    currentNamesrv: '10.0.0.10:9876'
  };

  it('ignores equivalent whitespace but detects a changed current selection or address list', () => {
    expect(isNameserverDraftDirty({
      namesrvAddrList: [' 10.0.0.10:9876 ', '10.0.0.11:9876'],
      currentNamesrv: '10.0.0.10:9876'
    }, saved)).toBe(false);
    expect(isNameserverDraftDirty({ ...saved, currentNamesrv: '10.0.0.11:9876' }, saved)).toBe(true);
    expect(isNameserverDraftDirty({
      ...saved,
      namesrvAddrList: ['10.0.0.10:9876']
    }, saved)).toBe(true);
  });
});
