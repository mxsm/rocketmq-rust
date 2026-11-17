// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::super::{model::*, service::KnowledgeService};
use crate::{
    PostgresRepository,
    api::capability_verification_test_state,
    auth::{AuthContext, AuthService},
};

const ISSUER: &str = "https://d22-fixture.invalid";
const AUDIENCE: &str = "rocketmq-sre-control-plane";
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

pub(crate) struct Fixture {
    pub repository: PostgresRepository,
    pub service: KnowledgeService,
    pub operator: AuthContext,
    pub cluster: ClusterId,
}

impl Fixture {
    pub async fn new() -> Self {
        let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit isolated PostgreSQL required");
        let repository = PostgresRepository::connect(&url, 4).await.unwrap();
        let cluster = ClusterId::new();
        let operator = AuthContext {
            tenant_id: TenantId::new(),
            subject: "d22-engineering-operator".to_owned(),
            clusters: BTreeSet::from([cluster]),
            roles: BTreeSet::from(["operator".to_owned()]),
        };
        insert_cluster(&repository, operator.tenant_id, cluster).await;
        Self {
            service: KnowledgeService::new(repository.clone()),
            repository,
            operator,
            cluster,
        }
    }

    pub fn document(&self, label: &str, component: &str, requirement: &str, markdown: &str) -> ImportKnowledgeRequest {
        let now = Utc::now();
        ImportKnowledgeRequest {
            cluster_id: Some(self.cluster),
            title: format!("Synthetic D22 {label}"),
            component: component.to_owned(),
            rocketmq_version_range: requirement.to_owned(),
            source_uri: format!("https://d22-fixture.invalid/{}/{label}", self.operator.tenant_id),
            source_version: "synthetic-corpus-v1".to_owned(),
            valid_from: Some(now - Duration::days(1)),
            valid_until: Some(now + Duration::days(1)),
            owner: "synthetic-source-owner".to_owned(),
            review_due_at: now + Duration::days(7),
            sensitivity: Sensitivity::Internal,
            review_status: KnowledgeReviewStatus::Validated,
            human_validated: true,
            ai_generated: false,
            markdown: markdown.to_owned(),
        }
    }

    pub async fn import(&self, document: ImportKnowledgeRequest) -> KnowledgeImportResult {
        self.service.import(&self.operator, document).await.unwrap()
    }

    pub fn query(&self, text: &str, version: &str, limit: u32) -> KnowledgeSearchQuery {
        KnowledgeSearchQuery {
            q: text.to_owned(),
            cluster_id: self.cluster,
            component: None,
            rocketmq_version: version.to_owned(),
            limit: Some(limit),
            include_unvalidated: false,
        }
    }

    pub async fn search(&self, text: &str, version: &str, limit: u32) -> KnowledgeSearchPage {
        self.service
            .search(&self.operator, &self.query(text, version, limit))
            .await
            .unwrap()
    }

    pub fn viewer(&self) -> AuthContext {
        AuthContext {
            subject: "d22-engineering-viewer".to_owned(),
            roles: BTreeSet::new(),
            ..self.operator.clone()
        }
    }

    pub fn router(&self) -> Router {
        let mut state = capability_verification_test_state(self.repository.clone()).unwrap();
        state.auth = AuthService::from_test_jwks(
            ISSUER,
            AUDIENCE,
            serde_json::from_value(
                json!({"keys":[{"kty":"RSA","kid":"d22-test","alg":"RS256","use":"sig","n":MODULUS,"e":"AQAB"}]}),
            )
            .unwrap(),
        );
        crate::phase1_api::public_routes().with_state(state)
    }

    pub async fn counts(&self) -> (i64, i64, i64, i64) {
        sqlx::query_as("SELECT
            (SELECT COUNT(*) FROM knowledge_items WHERE tenant_id=$1),
            (SELECT COUNT(*) FROM knowledge_chunks c JOIN knowledge_items k ON k.id=c.knowledge_item_id WHERE k.tenant_id=$1),
            (SELECT COUNT(*) FROM knowledge_review_tasks r JOIN knowledge_items k ON k.id=r.knowledge_item_id WHERE k.tenant_id=$1),
            (SELECT COUNT(*) FROM knowledge_feedback WHERE tenant_id=$1)")
            .bind(self.operator.tenant_id.as_uuid()).fetch_one(&self.repository.pool).await.unwrap()
    }
}

pub(super) async fn insert_cluster(repository: &PostgresRepository, tenant: TenantId, cluster: ClusterId) {
    sqlx::query("INSERT INTO clusters(id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,
        requested_access_profile,effective_access_profile,onboarding_state)
        VALUES($1,$2,$3,'synthetic','local','5.3.0','synthetic','d22-fixture','read_only','read_only','pending')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("d22-{cluster}"))
        .execute(&repository.pool).await.unwrap();
}

pub(super) async fn call(
    router: Router,
    actor: &AuthContext,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("d22-test".to_owned());
    let token = jsonwebtoken::encode(
        &header,
        &json!({"iss":ISSUER,"aud":AUDIENCE,"sub":actor.subject,
        "exp":Utc::now().timestamp()+3600,"scope":"rocketmq:read","roles":actor.roles,
        "rocketmq_tenant":actor.tenant_id,"rocketmq_clusters":actor.clusters}),
        &EncodingKey::from_rsa_pem(include_bytes!(
            "../../../../../deploy/dev/fixtures/oauth-private-key.pem"
        ))
        .unwrap(),
    )
    .unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(!value.to_string().contains("postgresql://"));
    (status, value)
}

pub(super) fn measured(case: &str, values: Value) {
    eprintln!(
        "D22_BASELINE {}",
        json!({"corpus":"d22.synthetic-reviewed-retrieval.v1","case":case,"observed":values})
    );
}
