export interface AclQueryParams {
  brokerName?: string;
  clusterName?: string;
  filter?: string;
  searchParam?: string;
  resource?: string;
}

export interface AclUserView {
  brokerName: string;
  brokerAddr: string;
  username: string;
  password?: string;
  userType?: string;
  userStatus?: string;
}

export interface AclUserUpsertRequest {
  brokerName?: string;
  clusterName?: string;
  username?: string;
  password: string;
  userType: string;
  userStatus?: string;
}

export interface AclMutationResult {
  message: string;
  targetCount: number;
}

export interface AclPolicyEntryView {
  resource?: string;
  actions: string[];
  sourceIps: string[];
  decision?: string;
}

export interface AclPolicyView {
  brokerName: string;
  brokerAddr: string;
  subject?: string;
  policyType?: string;
  entries: AclPolicyEntryView[];
}

export interface AclPolicyRequest {
  brokerName?: string;
  clusterName?: string;
  subject: string;
  policies: AclPolicyRequestPolicy[];
}

export interface AclPolicyRequestPolicy {
  policyType: string;
  entries: AclPolicyEntryRequest[];
}

export interface AclPolicyEntryRequest {
  resource: string[];
  actions: string[];
  sourceIps: string[];
  decision: string;
}
