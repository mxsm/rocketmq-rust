// Copyright 2025 The RocketMQ Rust Authors
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

//! RocketMQ semantic colors mapped onto the GPUI Component theme.

use gpui::{App, Hsla, rgb};
use gpui_component::{Theme, ThemeMode};

/// Semantic colors used by dashboard pages and composed components.
///
/// New UI should retrieve the mapped values through [`gpui_component::ActiveTheme`] instead of
/// embedding these values in render code.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RocketmqPalette {
    /// Main application background.
    pub background: Hsla,
    /// Title bar and navigation chrome.
    pub chrome: Hsla,
    /// Cards, dialogs, and other raised surfaces.
    pub card: Hsla,
    /// Separators and control borders.
    pub border: Hsla,
    /// Primary text.
    pub foreground: Hsla,
    /// Muted surface and text.
    pub muted: Hsla,
    /// Muted text.
    pub muted_foreground: Hsla,
    /// Primary action color.
    pub primary: Hsla,
    /// Success status color.
    pub success: Hsla,
    /// Warning status color.
    pub warning: Hsla,
    /// Destructive status color.
    pub danger: Hsla,
    /// Informational status color.
    pub info: Hsla,
    /// Keyboard focus ring color.
    pub focus_ring: Hsla,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct StatusTone {
    background: Hsla,
    hover: Hsla,
    active: Hsla,
    foreground: Hsla,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct StatusTones {
    success: StatusTone,
    warning: StatusTone,
    danger: StatusTone,
    info: StatusTone,
}

impl RocketmqPalette {
    /// Returns the dashboard's default dark palette.
    pub fn dark() -> Self {
        Self {
            background: rgb(0x0B0F14).into(),
            chrome: rgb(0x0E141B).into(),
            card: rgb(0x121922).into(),
            border: rgb(0x283442).into(),
            foreground: rgb(0xF4F7FA).into(),
            muted: rgb(0x18222D).into(),
            muted_foreground: rgb(0x9CA8B7).into(),
            primary: rgb(0x14B8A6).into(),
            success: rgb(0x22C55E).into(),
            warning: rgb(0xF59E0B).into(),
            danger: rgb(0xEF4444).into(),
            info: rgb(0x3B82F6).into(),
            focus_ring: rgb(0x2DD4BF).into(),
        }
    }
}

impl StatusTones {
    fn dark(palette: &RocketmqPalette) -> Self {
        Self {
            success: StatusTone {
                background: palette.success,
                hover: rgb(0x34D399).into(),
                active: rgb(0x16A34A).into(),
                foreground: rgb(0x07110B).into(),
            },
            warning: StatusTone {
                background: palette.warning,
                hover: rgb(0xFBBF24).into(),
                active: rgb(0xD97706).into(),
                foreground: rgb(0x1F1300).into(),
            },
            danger: StatusTone {
                background: palette.danger,
                hover: rgb(0xF87171).into(),
                active: rgb(0xE23D3D).into(),
                foreground: rgb(0x1A0707).into(),
            },
            info: StatusTone {
                background: palette.info,
                hover: rgb(0x60A5FA).into(),
                active: rgb(0x3478E8).into(),
                foreground: rgb(0x06101F).into(),
            },
        }
    }
}

/// Applies the RocketMQ default dark theme after `gpui_component::init` has registered its
/// global theme state.
pub fn apply_dark_theme(cx: &mut App) {
    Theme::change(ThemeMode::Dark, None, cx);

    let palette = RocketmqPalette::dark();
    let status_tones = StatusTones::dark(&palette);
    let theme = Theme::global_mut(cx);
    let colors = &mut theme.colors;

    colors.background = palette.background;
    colors.popover = palette.card;
    colors.popover_foreground = palette.foreground;
    colors.group_box = palette.card;
    colors.group_box_foreground = palette.foreground;
    colors.secondary = palette.card;
    colors.secondary_foreground = palette.foreground;
    colors.sidebar = palette.chrome;
    colors.sidebar_foreground = palette.foreground;
    colors.sidebar_accent = palette.muted;
    colors.sidebar_accent_foreground = palette.foreground;
    colors.title_bar = palette.chrome;

    colors.border = palette.border;
    colors.input = palette.border;
    colors.sidebar_border = palette.border;
    colors.title_bar_border = palette.border;
    colors.foreground = palette.foreground;
    colors.muted = palette.muted;
    colors.muted_foreground = palette.muted_foreground;
    colors.primary = palette.primary;
    colors.primary_hover = rgb(0x0D9488).into();
    colors.primary_active = rgb(0x0D9488).into();
    colors.success = status_tones.success.background;
    colors.success_hover = status_tones.success.hover;
    colors.success_active = status_tones.success.active;
    colors.success_foreground = status_tones.success.foreground;
    colors.warning = status_tones.warning.background;
    colors.warning_hover = status_tones.warning.hover;
    colors.warning_active = status_tones.warning.active;
    colors.warning_foreground = status_tones.warning.foreground;
    colors.danger = status_tones.danger.background;
    colors.danger_hover = status_tones.danger.hover;
    colors.danger_active = status_tones.danger.active;
    colors.danger_foreground = status_tones.danger.foreground;
    colors.info = status_tones.info.background;
    colors.info_hover = status_tones.info.hover;
    colors.info_active = status_tones.info.active;
    colors.info_foreground = status_tones.info.foreground;
    colors.ring = palette.focus_ring;
}

#[cfg(test)]
mod tests {
    use gpui::{Hsla, rgb};

    use super::{RocketmqPalette, StatusTone, StatusTones};

    fn relative_luminance(color: Hsla) -> f32 {
        fn linearize(component: f32) -> f32 {
            if component <= 0.04045 {
                component / 12.92
            } else {
                ((component + 0.055) / 1.055).powf(2.4)
            }
        }

        let color = color.to_rgb();
        0.2126 * linearize(color.r) + 0.7152 * linearize(color.g) + 0.0722 * linearize(color.b)
    }

    fn contrast_ratio(foreground: Hsla, background: Hsla) -> f32 {
        let foreground = relative_luminance(foreground);
        let background = relative_luminance(background);
        let (lighter, darker) = if foreground >= background {
            (foreground, background)
        } else {
            (background, foreground)
        };

        (lighter + 0.05) / (darker + 0.05)
    }

    fn assert_status_tone_is_accessible(name: &str, tone: StatusTone) {
        assert_ne!(tone.background, tone.hover, "{name} hover must be distinguishable");
        assert_ne!(tone.background, tone.active, "{name} active must be distinguishable");
        assert_ne!(
            tone.hover, tone.active,
            "{name} hover and active must be distinguishable"
        );

        for (state, background) in [
            ("normal", tone.background),
            ("hover", tone.hover),
            ("active", tone.active),
        ] {
            assert!(
                contrast_ratio(tone.foreground, background) >= 4.5,
                "{name} {state} foreground contrast must meet WCAG AA"
            );
        }
    }

    #[test]
    fn dark_palette_exposes_distinct_semantic_status_colors() {
        let palette = RocketmqPalette::dark();

        assert_eq!(palette.background, rgb(0x0B0F14).into());
        assert_eq!(palette.chrome, rgb(0x0E141B).into());
        assert_eq!(palette.card, rgb(0x121922).into());
        assert_eq!(palette.border, rgb(0x283442).into());
        assert_eq!(palette.foreground, rgb(0xF4F7FA).into());
        assert_eq!(palette.muted, rgb(0x18222D).into());
        assert_eq!(palette.muted_foreground, rgb(0x9CA8B7).into());
        assert_eq!(palette.primary, rgb(0x14B8A6).into());
        assert_eq!(palette.success, rgb(0x22C55E).into());
        assert_eq!(palette.warning, rgb(0xF59E0B).into());
        assert_eq!(palette.danger, rgb(0xEF4444).into());
        assert_eq!(palette.info, rgb(0x3B82F6).into());
        assert_eq!(palette.focus_ring, rgb(0x2DD4BF).into());
        assert_ne!(palette.success, palette.warning);
        assert_ne!(palette.warning, palette.danger);
        assert_ne!(palette.danger, palette.info);
    }

    #[test]
    fn status_tones_keep_foregrounds_accessible_across_interaction_states() {
        let tones = StatusTones::dark(&RocketmqPalette::dark());

        assert_status_tone_is_accessible("success", tones.success);
        assert_status_tone_is_accessible("warning", tones.warning);
        assert_status_tone_is_accessible("danger", tones.danger);
        assert_status_tone_is_accessible("info", tones.info);
    }
}
