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

use rocketmq_sre_contracts::PostmortemConclusion;

use super::PostmortemAssembly;

/// Renders one confirmed revision as bounded Markdown for validated knowledge.
#[must_use]
pub fn render_markdown(content: &PostmortemAssembly) -> String {
    let mut markdown = String::new();
    section(&mut markdown, "摘要", &content.summary);
    section(&mut markdown, "影响", &content.impact);
    section(&mut markdown, "检测", &content.detection);
    conclusions(&mut markdown, "根因", &content.root_causes);
    conclusions(&mut markdown, "促成因素", &content.contributing_factors);
    conclusions(&mut markdown, "关键结论", &content.conclusions);
    section(&mut markdown, "恢复过程", &content.recovery);
    strings(&mut markdown, "有效动作", &content.effective_actions);
    strings(&mut markdown, "无效动作", &content.ineffective_actions);
    markdown
}

fn section(markdown: &mut String, heading: &str, value: &str) {
    markdown.push_str("## ");
    markdown.push_str(heading);
    markdown.push_str("\n\n");
    markdown.push_str(value.trim());
    markdown.push_str("\n\n");
}

fn conclusions(markdown: &mut String, heading: &str, values: &[PostmortemConclusion]) {
    markdown.push_str("## ");
    markdown.push_str(heading);
    markdown.push_str("\n\n");
    if values.is_empty() {
        markdown.push_str("- 无\n\n");
        return;
    }
    for value in values {
        markdown.push_str("- ");
        markdown.push_str(value.statement.trim());
        markdown.push_str("（Evidence: ");
        for (index, evidence_id) in value.evidence_ids.iter().enumerate() {
            if index > 0 {
                markdown.push_str(", ");
            }
            markdown.push_str(&evidence_id.to_string());
        }
        markdown.push_str("）\n");
    }
    markdown.push('\n');
}

fn strings(markdown: &mut String, heading: &str, values: &[String]) {
    markdown.push_str("## ");
    markdown.push_str(heading);
    markdown.push_str("\n\n");
    if values.is_empty() {
        markdown.push_str("- 无\n\n");
        return;
    }
    for value in values {
        markdown.push_str("- ");
        markdown.push_str(value.trim());
        markdown.push('\n');
    }
    markdown.push('\n');
}
