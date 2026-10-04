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

//! Palette, glyphs, and color math.
//!
//! The interface paints its own background, so it reads the same on light and dark
//! terminal themes. Every color is 24-bit; [`ColorDepth::Indexed`] terminals get the
//! nearest xterm-256 entry in a final pass over the frame.

use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;

use crate::commands::RiskLevel;

pub(crate) const BACKGROUND: Color = Color::Rgb(11, 15, 26);
pub(crate) const SURFACE: Color = Color::Rgb(19, 26, 43);
pub(crate) const SURFACE_RAISED: Color = Color::Rgb(30, 39, 62);
/// Background of the row or segment that keyboard input acts on.
pub(crate) const SELECTION: Color = Color::Rgb(52, 39, 94);
pub(crate) const STRIPE: Color = Color::Rgb(14, 19, 32);
pub(crate) const BORDER: Color = Color::Rgb(46, 55, 82);
pub(crate) const TEXT: Color = Color::Rgb(208, 214, 228);
pub(crate) const BRIGHT: Color = Color::Rgb(242, 244, 250);
pub(crate) const MUTED: Color = Color::Rgb(153, 163, 185);
pub(crate) const FAINT: Color = Color::Rgb(98, 109, 136);
pub(crate) const PRIMARY: Color = Color::Rgb(192, 132, 252);
pub(crate) const PRIMARY_DEEP: Color = Color::Rgb(147, 82, 235);
pub(crate) const CYAN: Color = Color::Rgb(34, 211, 238);
pub(crate) const SUCCESS: Color = Color::Rgb(74, 222, 128);
pub(crate) const WARNING: Color = Color::Rgb(251, 191, 36);
pub(crate) const DANGER: Color = Color::Rgb(248, 113, 113);

pub(crate) const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
pub(crate) const GLYPH_BRAND: &str = "◆";
pub(crate) const GLYPH_EXPANDED: &str = "▾";
pub(crate) const GLYPH_COLLAPSED: &str = "▸";
pub(crate) const GLYPH_SELECTED: &str = "▌";
pub(crate) const GLYPH_POINTER: &str = "▸";
pub(crate) const GLYPH_OK: &str = "✓";
pub(crate) const GLYPH_FAIL: &str = "×";
pub(crate) const GLYPH_STOP: &str = "■";
pub(crate) const GLYPH_ATTENTION: &str = "▲";
pub(crate) const GLYPH_RUN: &str = "▶";
pub(crate) const GLYPH_DOT: &str = "●";
pub(crate) const GLYPH_RING: &str = "○";
pub(crate) const GLYPH_PATH: &str = "›";
pub(crate) const GLYPH_MASK: &str = "•";
pub(crate) const RULE_THIN: &str = "─";
pub(crate) const RULE_HEAVY: &str = "━";
pub(crate) const SCROLL_THUMB: &str = "┃";

pub(crate) fn text() -> Style {
    Style::new().fg(TEXT)
}

pub(crate) fn bright() -> Style {
    Style::new().fg(BRIGHT).add_modifier(Modifier::BOLD)
}

pub(crate) fn muted() -> Style {
    Style::new().fg(MUTED)
}

pub(crate) fn faint() -> Style {
    Style::new().fg(FAINT)
}

pub(crate) fn accent() -> Style {
    Style::new().fg(PRIMARY).add_modifier(Modifier::BOLD)
}

pub(crate) fn colored(color: Color) -> Style {
    Style::new().fg(color)
}

pub(crate) fn bold(color: Color) -> Style {
    Style::new().fg(color).add_modifier(Modifier::BOLD)
}

/// Returns the frame of the activity spinner for `tick`.
pub(crate) fn spinner(tick: u64) -> &'static str {
    SPINNER[(tick / 2) as usize % SPINNER.len()]
}

pub(crate) fn risk_color(risk: RiskLevel) -> Color {
    match risk {
        RiskLevel::Safe => SUCCESS,
        RiskLevel::Mutating => WARNING,
        RiskLevel::Dangerous => DANGER,
    }
}

/// Returns the marker of a risk level. The shapes differ so color is not the only cue.
pub(crate) fn risk_glyph(risk: RiskLevel) -> &'static str {
    match risk {
        RiskLevel::Safe => "○",
        RiskLevel::Mutating => "◆",
        RiskLevel::Dangerous => "▲",
    }
}

/// Returns the risk level as a badge: its marker followed by its name in capitals.
pub(crate) fn risk_badge(risk: RiskLevel) -> String {
    format!("{} {}", risk_glyph(risk), risk.as_str().to_ascii_uppercase())
}

/// Blends `from` toward `to`; `amount` is clamped to `0.0..=1.0`.
///
/// Colors without RGB components cannot be blended and yield `to`.
pub(crate) fn mix(from: Color, to: Color, amount: f32) -> Color {
    let (Color::Rgb(from_red, from_green, from_blue), Color::Rgb(to_red, to_green, to_blue)) = (from, to) else {
        return to;
    };
    let amount = amount.clamp(0.0, 1.0);
    let channel = |start: u8, end: u8| {
        let value = f32::from(start) + (f32::from(end) - f32::from(start)) * amount;
        value.round() as u8
    };
    Color::Rgb(
        channel(from_red, to_red),
        channel(from_green, to_green),
        channel(from_blue, to_blue),
    )
}

/// Returns the brand gradient, violet at `0.0` through cyan at `1.0`.
pub(crate) fn gradient(position: f32) -> Color {
    mix(PRIMARY, CYAN, position)
}

/// Color capability of the output terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ColorDepth {
    #[default]
    TrueColor,
    /// The xterm 256-color palette.
    Indexed,
}

impl ColorDepth {
    /// Detects the color depth from the environment of the process.
    pub(crate) fn detect() -> Self {
        if cfg!(windows) {
            // Windows Terminal and the Windows 10+ console host render 24-bit color and
            // do not advertise it through an environment variable.
            Self::TrueColor
        } else {
            Self::from_colorterm(std::env::var("COLORTERM").ok().as_deref())
        }
    }

    /// Interprets a `COLORTERM` value; only an explicit 24-bit claim selects true color.
    pub(crate) fn from_colorterm(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some(value) if value.eq_ignore_ascii_case("truecolor") || value.eq_ignore_ascii_case("24bit") => {
                Self::TrueColor
            }
            _ => Self::Indexed,
        }
    }
}

const CUBE_LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];

/// Maps an RGB color to the nearest xterm-256 palette entry; other colors pass through.
pub(crate) fn to_indexed(color: Color) -> Color {
    let Color::Rgb(red, green, blue) = color else {
        return color;
    };

    let nearest_level = |value: u8| {
        CUBE_LEVELS
            .iter()
            .enumerate()
            .min_by_key(|(_, level)| value.abs_diff(**level))
            .map_or(0, |(index, _)| index)
    };
    let (red_index, green_index, blue_index) = (nearest_level(red), nearest_level(green), nearest_level(blue));
    let cube = (
        CUBE_LEVELS[red_index],
        CUBE_LEVELS[green_index],
        CUBE_LEVELS[blue_index],
    );

    // The grayscale ramp is 232..=255 with levels 8, 18, ..., 238.
    let average = (u16::from(red) + u16::from(green) + u16::from(blue)) / 3;
    let gray_index = (average.saturating_sub(8) + 5) / 10;
    let gray_index = gray_index.min(23) as u8;
    let gray_level = 8 + 10 * gray_index;

    let distance = |candidate: (u8, u8, u8)| {
        let delta = |a: u8, b: u8| u32::from(a.abs_diff(b)).pow(2);
        delta(red, candidate.0) + delta(green, candidate.1) + delta(blue, candidate.2)
    };
    if distance((gray_level, gray_level, gray_level)) < distance(cube) {
        Color::Indexed(232 + gray_index)
    } else {
        Color::Indexed((16 + 36 * red_index + 6 * green_index + blue_index) as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_interpolates_rgb_and_passes_other_colors_through() {
        let black = Color::Rgb(0, 0, 0);
        let white = Color::Rgb(200, 100, 50);

        assert_eq!(mix(black, white, 0.0), black);
        assert_eq!(mix(black, white, 1.0), white);
        assert_eq!(mix(black, white, 0.5), Color::Rgb(100, 50, 25));
        assert_eq!(mix(black, white, 9.0), white);
        assert_eq!(mix(Color::Reset, white, 0.2), white);
        assert_eq!(mix(black, Color::Reset, 0.2), Color::Reset);
    }

    #[test]
    fn color_depth_requires_an_explicit_truecolor_claim() {
        assert_eq!(ColorDepth::from_colorterm(Some("truecolor")), ColorDepth::TrueColor);
        assert_eq!(ColorDepth::from_colorterm(Some(" 24BIT ")), ColorDepth::TrueColor);
        assert_eq!(ColorDepth::from_colorterm(Some("yes")), ColorDepth::Indexed);
        assert_eq!(ColorDepth::from_colorterm(None), ColorDepth::Indexed);
    }

    #[test]
    fn indexed_conversion_picks_the_nearest_palette_entry() {
        assert_eq!(to_indexed(Color::Rgb(0, 0, 0)), Color::Indexed(16));
        assert_eq!(to_indexed(Color::Rgb(255, 255, 255)), Color::Indexed(231));
        assert_eq!(to_indexed(Color::Rgb(255, 0, 0)), Color::Indexed(196));
        assert_eq!(to_indexed(Color::Rgb(128, 128, 128)), Color::Indexed(244));
        assert_eq!(to_indexed(Color::Reset), Color::Reset);
        // Every theme color maps into the palette without leaving the valid index range.
        for color in [BACKGROUND, SURFACE, SELECTION, BORDER, TEXT, PRIMARY, CYAN, DANGER] {
            assert!(matches!(to_indexed(color), Color::Indexed(index) if index >= 16));
        }
    }

    #[test]
    fn risk_levels_have_distinct_markers() {
        let glyphs = [RiskLevel::Safe, RiskLevel::Mutating, RiskLevel::Dangerous].map(risk_glyph);

        assert_ne!(glyphs[0], glyphs[1]);
        assert_ne!(glyphs[1], glyphs[2]);
        assert_ne!(glyphs[0], glyphs[2]);
    }
}
