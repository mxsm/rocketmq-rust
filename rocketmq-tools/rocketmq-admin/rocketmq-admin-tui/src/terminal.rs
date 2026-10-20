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

//! Terminal mode ownership: raw mode, alternate screen, mouse capture, and paste.

use std::io;
use std::io::stdout;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crossterm::event::DisableMouseCapture;
use crossterm::event::EnableMouseCapture;
use crossterm::execute;
use crossterm::queue;
use crossterm::terminal::enable_raw_mode;
use crossterm::terminal::BeginSynchronizedUpdate;
use crossterm::terminal::EndSynchronizedUpdate;
use crossterm::terminal::EnterAlternateScreen;
use ratatui::DefaultTerminal;
use ratatui::Frame;

/// Set by the panic hook after it restored the terminal underneath a live application.
static DISTURBED: AtomicBool = AtomicBool::new(false);

/// Enters the alternate screen with raw input, mouse capture, and bracketed paste.
///
/// # Errors
///
/// Returns the I/O error of the first terminal mode that could not be enabled. The
/// terminal is restored before the error is returned.
pub(crate) fn enter() -> io::Result<DefaultTerminal> {
    let terminal = ratatui::try_init()?;
    install_panic_hook();
    if let Err(error) = enable_input_modes(&mut stdout(), true) {
        // Best effort: the original failure is the one worth reporting.
        let _ = leave();
        return Err(error);
    }
    Ok(terminal)
}

/// Restores the terminal to the state it had before [`enter`].
///
/// # Errors
///
/// Returns the first I/O error raised while leaving the terminal modes. Every mode is
/// still attempted so one failure does not leave the others enabled.
pub(crate) fn leave() -> io::Result<()> {
    let input_modes = disable_input_modes(&mut stdout());
    ratatui::try_restore()?;
    input_modes
}

/// Draws one frame as a single terminal update.
///
/// A terminal that supports synchronized output presents the frame once it is
/// complete instead of while it is still being written, so an animated frame never
/// shows half painted. Other terminals ignore the two markers.
///
/// # Errors
///
/// Returns the I/O error raised while writing the frame. The update is closed even
/// then, so a failed frame cannot leave the terminal frozen.
pub(crate) fn draw(terminal: &mut DefaultTerminal, render: impl FnOnce(&mut Frame)) -> io::Result<()> {
    queue!(terminal.backend_mut(), BeginSynchronizedUpdate)?;
    let drawn = terminal.draw(render).map(|_| ());
    let closed = execute!(terminal.backend_mut(), EndSynchronizedUpdate);
    drawn.and(closed)
}

/// Starts or stops mouse reporting.
///
/// While reporting is off the terminal handles the mouse itself, which is what makes
/// native text selection and copy available.
///
/// # Errors
///
/// Returns the I/O error raised while writing the terminal command.
pub(crate) fn set_mouse_capture(writer: &mut impl Write, enabled: bool) -> io::Result<()> {
    if enabled {
        execute!(writer, EnableMouseCapture)
    } else {
        execute!(writer, DisableMouseCapture)
    }
}

/// Re-enters the terminal modes after a panic hook restored them mid-session.
///
/// A panicking command task is caught and reported as a failed command, but the
/// process-wide panic hook has already left raw mode and the alternate screen by
/// then. Returns whether the terminal had to be re-entered.
///
/// # Errors
///
/// Returns the I/O error raised while re-entering a terminal mode.
pub(crate) fn recover_if_disturbed(terminal: &mut DefaultTerminal, mouse_capture: bool) -> io::Result<bool> {
    if !DISTURBED.swap(false, Ordering::AcqRel) {
        return Ok(false);
    }
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;
    enable_input_modes(&mut stdout(), mouse_capture)?;
    terminal.clear()?;
    Ok(true)
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // Runs before ratatui's hook, which leaves raw mode and the alternate screen.
        let _ = disable_input_modes(&mut stdout());
        DISTURBED.store(true, Ordering::Release);
        previous(info);
    }));
}

fn enable_input_modes(writer: &mut impl Write, mouse_capture: bool) -> io::Result<()> {
    if mouse_capture {
        execute!(writer, EnableMouseCapture)?;
    }
    enable_bracketed_paste(writer)
}

fn disable_input_modes(writer: &mut impl Write) -> io::Result<()> {
    let mouse = execute!(writer, DisableMouseCapture);
    disable_bracketed_paste(writer)?;
    mouse
}

// Bracketed paste delivers a clipboard as one event, so a pasted line break can never
// act as Enter. The Windows console input API has no such mode: there a paste arrives
// as ordinary key events.
#[cfg(not(windows))]
fn enable_bracketed_paste(writer: &mut impl Write) -> io::Result<()> {
    execute!(writer, crossterm::event::EnableBracketedPaste)
}

#[cfg(not(windows))]
fn disable_bracketed_paste(writer: &mut impl Write) -> io::Result<()> {
    execute!(writer, crossterm::event::DisableBracketedPaste)
}

#[cfg(windows)]
fn enable_bracketed_paste(_writer: &mut impl Write) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn disable_bracketed_paste(_writer: &mut impl Write) -> io::Result<()> {
    Ok(())
}
