# Live Presence

Status: local browser viewport, visible-context bridge, and bounded automatic meeting-note checkpoints implemented on 2026-08-17. Marcus has a dedicated Chrome profile, a live browser view in `/visualizer.html`, Mark/MARCUS control ownership, bounded navigation/input commands, approved-site visible-text observation, automatic silent live-context forwarding to Realtime, the live-presence readiness model, setup API, setup console, OpenAI Realtime voice path, desktop-context awareness, and concise Obsidian conversation-note writes through the desktop agent. Platform login/MFA, virtual audio routing, visible Zoom identity, and first live-call acceptance still require completion.

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
- Marcus chat tools: `marcus_browser_status`, `marcus_browser_open`, and non-consequential exact-visible-control `marcus_browser_activate`
- Current-page inspection tool: `marcus_browser_read` scans up to 12 rendered viewports on approved sites and restores the original page position
- Visible-context allowlist: Gmail for explicit browser inspection; Zoom, Skool, Google Meet, Teams, YouTube, and TikTok for live Realtime context
- Automatic meeting-note checkpoint: `/api/marcus/meeting-notes/checkpoint` to the local `docs/marcus/conversations/` writer
- Call/audio console: `/call-marcus.html` (`/obs-marcus.html` remains a compatible legacy path)

## Operating Model

The local browser session is the identity and platform-presence layer. It is not the whole brain.

The durable runtime is:

`dedicated Marcus browser profile -> virtual audio route -> Marcus Realtime voice -> Marcus operator tools -> meeting memory and follow-up actions`

OBS Studio is optional and only needed for scenes, streaming, avatars, overlays, or advanced mixing. `/call-marcus.html` is the normal Zoom audio/Realtime sidecar and does not require the OBS desktop application. Notes, decisions, action items, and follow-up drafts must land in Marcus memory.

The OBS sidecar now keeps a bounded transcript in page memory, checkpoints every five minutes, and sends a final checkpoint when stopped. The server uses the existing transcript analyzer and queues only concise derived notes to the desktop agent. Raw transcript dumps are not written to the vault or durable action queue.

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
- Install VB-CABLE, reboot Windows, and confirm the VB-Audio playback endpoint plus `CABLE Output` appear.
- Keep Mark microphone separate from Marcus output.
- Select a dedicated Marcus microphone/input route in Zoom or the live platform.
- In `/call-marcus.html`, select `Speakers (VB-Audio Virtual Cable)` for MARCUS voice output. Older driver packages may call this `CABLE Input`.
- In Zoom, select `CABLE Output` as MARCUS's microphone and keep Zoom speakers on Mark's headphones.
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

The desktop agent captures a compressed page viewport, not Chrome profile storage, and relays it to the authenticated server. On an explicit site allowlist it also extracts at most 6,000 characters of rendered text currently inside the viewport. The extractor skips inputs, textareas, selects, editable regions, hidden elements, and off-screen text. Password fields suspend the frame, clear visible context, and reject remote typing; Mark completes password, MFA, captcha, recovery, camera, and microphone permission steps in the visible MARCUS Chrome window. The server never receives cookies, saved passwords, browser storage, raw HTML, unrestricted DOM content, or form values. Context exists only in the short-lived browser relay cache.

`Open` creates a new Chrome tab and preserves the current page. `Navigate` replaces only the active MARCUS tab. Gmail context is available to explicit authenticated browser inspection but is never automatically injected into a live voice session. Visible Zoom, Skool, Google Meet, Teams, YouTube, and TikTok text is forwarded silently to an active Realtime session only when it changes.

`/visualizer.html` starts in Browser mode. `Take control` gives Mark the remote click, scroll, typing, address, back, forward, and refresh channel. `Return control` gives the channel back to Marcus. Marcus can inspect browser status and open an exact HTTP(S) URL only from Mark's direct current request. Consequential page actions and external communication retain their existing approval rules.

When Mark asks Marcus to inspect, analyze, browse, scan, summarize, give feedback on, or look through the page already open, Marcus should call `marcus_browser_read`; an exact URL is not required. The tool reads bounded rendered text across up to 12 viewports, excludes form/editable/hidden content, and restores the original scroll position. It was locally verified against eight ScoopOS community viewports on 2026-08-17.

Use only that Chrome window for `markgromermarcus@gmail.com`, Gmail, Skool, Zoom, YouTube, and TikTok sessions. Do not paste passwords into chat or store them in the repo. Mark should do the first login, MFA, captcha, and account recovery steps directly in the browser.

## Audio Architecture

Audio is the reliability risk. Browser automation alone is not enough.

Required channels:

- Meeting output to Marcus ears.
- Marcus voice output to the platform's microphone input.
- Mark microphone to the platform as Mark.
- Optional OBS monitor/mix route for demos.

The one-PC Zoom route is deliberately split per audio element:

`Zoom tab output -> Mark's headphones`

`Zoom tab shared audio -> MARCUS Call Console -> Realtime input`

`Realtime output audio element -> Speakers (VB-Audio Virtual Cable) -> CABLE Output -> Zoom microphone`

This avoids routing the entire Chrome process through VB-CABLE, which would echo Zoom participants back into the call. The Call Console uses `HTMLMediaElement.setSinkId()` and automatically prefers Pack45's `Speakers (VB-Audio Virtual Cable)` stereo output, then older `CABLE Input` labels. Zoom must use `CABLE Output` as MARCUS's microphone while keeping its speaker on Mark's headphones.

The official VB-CABLE package is available from [VB-Audio](https://vb-audio.com/Cable/index.htm). Signed Pack45 was installed successfully on 2026-08-17. `Speakers (VB-Audio Virtual Cable)`, `CABLE In 16 Ch`, and `CABLE Output` all reported `OK` before reboot. The vendor-required reboot and post-reboot audio acceptance remain pending until current work is saved.

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

- Add platform-specific page adapters if generic rendered-text observation misses virtualized or iframe-hosted chat.
- Add real device enumeration from the desktop agent so setup can verify audio routes instead of relying only on manual checklist items.
- Link automatically created meeting summaries to known project, person, and client notes after entity resolution.
- Add OBS scene state and emergency mute indicators.

Related: [[external-presence]], [[voice-interface]], [[access-model]], [[execution-loop]], [[context-memory]].
