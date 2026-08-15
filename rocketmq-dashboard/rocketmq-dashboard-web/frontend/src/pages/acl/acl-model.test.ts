import type { AclPolicyView, AclUserView } from '../../types/acl';
import type { BrokerInfo } from '../../types/broker';
import {
  buildAclPolicyRequest,
  createAclScopeQuery,
  deriveAclScopeOptions,
  filterAclPolicyRows,
  filterAclUsers,
  flattenAclPolicies
} from './acl-model';

const brokers: BrokerInfo[] = [
  { clusterName: 'Cluster-B', brokerName: 'broker-b', brokerId: 0, address: '10.0.0.2:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
  { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
  { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 1, address: '10.0.0.3:10911', role: 'SLAVE', version: '5.3.0', produceTps: 0, consumeTps: 0 }
];

describe('ACL scope model', () => {
  it('derives sorted unique cluster and broker options for the selected cluster', () => {
    expect(deriveAclScopeOptions(brokers, 'Cluster-A')).toEqual({
      clusters: [
        { value: 'Cluster-A', label: 'Cluster-A' },
        { value: 'Cluster-B', label: 'Cluster-B' }
      ],
      brokers: [{ value: 'broker-a', label: 'broker-a' }]
    });
  });

  it('only creates list query parameters for a confirmed coherent broker scope', () => {
    expect(createAclScopeQuery({ clusterName: 'Cluster-A', brokerName: 'broker-a' }, brokers)).toEqual({
      clusterName: 'Cluster-A',
      brokerName: 'broker-a'
    });
    expect(createAclScopeQuery({ clusterName: 'Cluster-A', brokerName: 'broker-b' }, brokers)).toBeNull();
    expect(createAclScopeQuery(null, brokers)).toBeNull();
  });
});

describe('ACL filters', () => {
  const users: AclUserView[] = [
    { brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'rocketmq-admin', userType: 'Super', userStatus: 'enable' },
    { brokerName: 'broker-b', brokerAddr: '10.0.0.2:10911', username: 'reader', userType: 'Normal', userStatus: 'disable' }
  ];
  const policies: AclPolicyView[] = [
    {
      brokerName: 'broker-a',
      brokerAddr: '10.0.0.1:10911',
      subject: 'User:rocketmq-admin',
      policyType: 'Custom',
      entries: [{ resource: 'Topic:Orders', actions: ['Pub'], sourceIps: ['10.0.0.0/24'], decision: 'Allow' }]
    },
    {
      brokerName: 'broker-b',
      brokerAddr: '10.0.0.2:10911',
      subject: 'User:reader',
      policyType: 'Custom',
      entries: [{ resource: 'Topic:Payments', actions: ['Sub'], sourceIps: ['192.168.1.1'], decision: 'Deny' }]
    }
  ];

  it('filters users without mutating the API array', () => {
    const snapshot = structuredClone(users);
    expect(filterAclUsers(users, 'admin')).toEqual([users[0]]);
    expect(users).toEqual(snapshot);
  });

  it('flattens and filters policy rows without mutating API records or row arrays', () => {
    const snapshot = structuredClone(policies);
    const rows = flattenAclPolicies(policies);
    const rowSnapshot = structuredClone(rows);
    expect(filterAclPolicyRows(rows, '192.168.1.1')).toEqual([rows[1]]);
    expect(policies).toEqual(snapshot);
    expect(rows).toEqual(rowSnapshot);
    expect(rows[0].actions).not.toBe(policies[0].entries[0].actions);
    expect(rows[0].sourceIps).not.toBe(policies[0].entries[0].sourceIps);
  });
});

describe('ACL policy draft parsing', () => {
  it('maps comma-separated resource and source IP values to the current policy request DTO', () => {
    expect(buildAclPolicyRequest(
      { clusterName: 'Cluster-A', brokerName: 'broker-a' },
      {
        subject: ' User:rocketmq-admin ',
        policyType: 'Custom',
        resources: ' Topic:Orders, Group:billing ',
        actions: ['Pub', 'Sub'],
        sourceIps: ' 10.0.0.1, 2001:db8::1 ',
        decision: 'Allow'
      }
    )).toEqual({
      ok: true,
      value: {
        clusterName: 'Cluster-A',
        brokerName: 'broker-a',
        subject: 'User:rocketmq-admin',
        policies: [{
          policyType: 'Custom',
          entries: [{
            resource: ['Topic:Orders', 'Group:billing'],
            actions: ['Pub', 'Sub'],
            sourceIps: ['10.0.0.1', '2001:db8::1'],
            decision: 'Allow'
          }]
        }]
      }
    });
  });

  it('retains sibling entries when building an update for one flattened policy row', () => {
    const [selectedRow] = flattenAclPolicies([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:payments', policyType: 'Custom',
      entries: [
        { resource: 'Topic:Orders', actions: ['Pub'], sourceIps: ['10.0.0.1'], decision: 'Allow' },
        { resource: 'Group:billing', actions: ['Sub'], sourceIps: ['10.0.0.2'], decision: 'Deny' }
      ]
    }]);

    expect(buildAclPolicyRequest(
      { clusterName: 'Cluster-A', brokerName: 'broker-a' },
      {
        subject: 'User:payments', policyType: 'Custom', resources: 'Topic:Orders', actions: ['Pub', 'Sub'],
        sourceIps: '10.0.0.1', decision: 'Allow'
      },
      selectedRow
    )).toEqual({
      ok: true,
      value: {
        clusterName: 'Cluster-A', brokerName: 'broker-a', subject: 'User:payments',
        policies: [{
          policyType: 'Custom',
          entries: [
            { resource: ['Topic:Orders'], actions: ['Pub', 'Sub'], sourceIps: ['10.0.0.1'], decision: 'Allow' },
            { resource: ['Group:billing'], actions: ['Sub'], sourceIps: ['10.0.0.2'], decision: 'Deny' }
          ]
        }]
      }
    });
  });
});
