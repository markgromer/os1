# Live Presence

Status: local browser viewport bridge implemented and verified on 2026-08-17. Marcus has a dedicated logged-in Chrome profile, a live browser view in `/visualizer.html`, Mark/MARCUS control ownership, bounded navigation/input commands, the live-presence readiness model, setup API, setup console, OpenAI Realtime voice path, and desktop-context awareness. Platform MFA, virtual audio routing, visible Zoom identity, and first live-call acceptance still require Mark's hands-on setup on the PC.

## Purpose

Live presence is Marcus operating from Mark's PC as a visible assistant in normal browser sessions. The goal is live participation: Marcus can hear, answer, read chat, write notes, and support Zoom, Skool, YouTube Live, TikTok Live, and similar conversations without pretending to be a human or bypassing platform controls.

## Implemented App Pieces

- Readiness status: `/api/marcus/live-presence/status`
- Setup update: `/api/marcus/live-presence/setup`
- Setup console: `/live-presence.html`
- Realtime voice session: `/api/marcus/realtime/client-secret`
- Realtime telemetry: `/api/marcus/realtime/telemetry`
- Voice acceptance: `/api/marcus/realtime/acceptance`
- Marcus Live shell: `/live.html`
- Desktop context relay: existing desktop agent relay and dashboard APIs
- Local browser launcher: `scripts/launch-marcus-browser.ps1`
- Local browser controller: `desktop-marcus-browser.cjs`
- Browser status/frame/control/actions: `/api/marcus/browser/status`, `/api/marcus/browser/frame`, `/api/marcus/browser/control`, and `/api/marcus/browser/actions`
- Browser relay intake: `/api/marcus/browser/relay`
- Marcus chat tools: `marcus_browser_status` and `marcus_browser_open`

## Operating Model

The local browser session is the identity and platform-presence layer. It is not the whole brain.

The durable runtime is:

`dedicated Marcus browser profile -> virtual audio route -> Marcus Realtime voice -> Marcus operator tools -> meeting memory and follow-up actions`

OBS is a stage surface. It can show Marcus visually, but it is not the source of truth. Notes, decisions, action items, and follow-up drafts must land in Marcus memory.

## Modes

- `silent_shadow`: Marcus hears and reads, captures notes, and does not speak.
- `private_copilot`: Marcus suggests to Mark privately.
- `public_push_to_talk`: Marcus speaks only when Mark intentionally triggers him.
- `public_auto_reply`: Marcus may answer when clearly addressed as Marcus.
- `show_mode`: Marcus is allowed to be more presentational for demos and live streams.

Default initial mode: `public_push_to_talk`.

## Mark Setup Checklist

Mark owns these first-time setup items:

- Create a dedicated Chrome or Edge profile named `Marcus`.
- Use `scripts/launch-marcus-browser.ps1` to open the isolated Marcus browser profile.
- Sign into `markgromermarcus@gmail.com` in the Marcus Chrome window.
- Verify Marcus can read, compose, and send a Gmail test message from that browser session.
- Log into Zoom, Skool, YouTube, TikTok, and other platforms in that profile.
- Handle MFA, captcha, recovery, and identity-sensitive prompts directly.
- Set the visible display name to `Marcus - Mark's AI Assistant` where possible.
- Install and configure a virtual audio route such as VB-CABLE or VoiceMeeter.
- Keep Mark microphone separate from Marcus output.
- Select a dedicated Marcus microphone/input route in Zoom or the live platform.
- Rehearse emergency controls: mute Marcus, stop speaking, private-only mode, and leave meeting.

The setup console at `/live-presence.html` is the checklist Mark should use while configuring the PC.

## Marcus Browser Profile

Run this from the repo root:

```powershell
.\scripts\launch-marcus-browser.ps1
```

The launcher opens Chrome with Marcus's isolated local browser profile. It uses this profile directory:

```text
%LOCALAPPDATA%\M.A.R.C.U.S\MarcusBrowserProfile
```

The launcher binds Chrome DevTools Protocol to `127.0.0.1:9333`. The port is localhost-only and the bridge refuses a reachable endpoint unless it identifies itself as Chrome or Chromium. Port `9229` is intentionally not used because the current PC uses it for a Cloudflare Worker debugger.

The desktop agent captures a compressed page viewport, not Chrome profile storage, and relays it to the authenticated server. Password fields suspend the frame and reject remote typing; Mark completes password, MFA, captcha, recovery, camera, and microphone permission steps in the visible MARCUS Chrome window. The server never receives cookies, saved passwords, browser storage, or raw DOM content.

`/visualizer.html` starts in Browser mode. `Take control` gives Mark the remote click, scroll, typing, address, back, forward, and refresh channel. `Return control` gives the channel back to Marcus. Marcus can inspect browser status and open an exact HTTP(S) URL only from Mark's direct current request. Consequential page actions and external communication retain their existing approval rules.

Use only that Chrome window for `markgromermarcus@gmail.com`, Gmail, Skool, Zoom, YouTube, and TikTok sessions. Do not paste passwords into chat or store them in the repo. Mark should do the first login, MFA, captcha, and account recovery steps directly in the browser.

## Audio Architecture

Audio is the reliability risk. Browser automation alone is not enough.

Required channels:

- Meeting output to Marcus ears.
- Marcus voice output to the platform's microphone input.
- Mark microphone to the platform as Mark.
- Optional OBS monitor/mix route for demos.

The first production setup can run on one PC with virtual devices, but a second machine or VM is the stronger long-term setup for bulletproof calls because it prevents echo, device contention, and browser focus conflicts.

## Platform Notes

Zoom:

- Join from the Marcus browser profile or browser app.
- Use visible assistant identity.
- Use `silent_shadow`, `private_copilot`, or `public_push_to_talk` until echo and interruption are proven.

Skool:

- Use local browser profile observation and drafting first.
- Do not automate posting or messaging until platform permission and identity rules are clear.

YouTube Live:

- Local browser presence works for demos.
- Official YouTube Live Chat API is preferred later for chat when properly connected.

TikTok Live:

- Local browser presence is the initial path.
- API-style integration should wait for approved platform access.

## Readiness Gates

Private copilot is acceptable when:

- Desktop agent is online.
- Marcus browser profile is ready.
- Realtime voice is configured.
- Basic audio route exists.
- Emergency controls are understood.

Public voice is acceptable when:

- All required setup items in `/live-presence.html` are complete.
- Mark has tested echo control.
- Marcus is visible as an AI assistant.
- Mark has an immediate mute/stop path.

## Open Work

- Add visible-page and Zoom/Skool chat observers that produce bounded evidence without relaying credentials or unrestricted DOM content.
- Add real device enumeration from the desktop agent so setup can verify audio routes instead of relying only on manual checklist items.
- Add meeting memory routes for active live sessions if not present in the current deployment branch.
- Add OBS scene state and emergency mute indicators.

Related: [[external-presence]], [[voice-interface]], [[access-model]], [[execution-loop]], [[context-memory]].
