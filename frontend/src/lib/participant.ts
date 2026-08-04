const KEY = 'dropvault_participant_id'

/**
 * Returns a stable participant ID for this browser tab, generating one the
 * first time it's needed. Deliberately uses sessionStorage, not
 * localStorage: sessionStorage is per-tab, so opening a second tab
 * naturally gets a distinct participant identity — exactly what's needed
 * to test multi-peer behavior locally (two tabs = two people), without any
 * extra setup. A refresh within the same tab correctly keeps the same
 * identity (and therefore the same delivery history), matching how the
 * original P2P project's peerId worked.
 */
export function getParticipantId(): string {
  let id = sessionStorage.getItem(KEY)
  if (!id) {
    id = crypto.randomUUID()
    sessionStorage.setItem(KEY, id)
  }
  return id
}
