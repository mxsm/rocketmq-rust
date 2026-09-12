# Account and authentication UI

The account page uses the dashboard's shared dark tokens, controls and status badges. Missing account data does not imply an active account. A failed refresh retains the previous details and identifies them as an earlier observation. There is one password-change action and one current-session sign-out action; session management remains embedded with its own refresh control.

The password dialog uses the shared modal primitive for focus containment, return focus, Escape and backdrop behavior. A required initial password change cannot be dismissed into the dashboard. Current/new/confirmation fields have associated labels and autocomplete metadata. Pending changes disable inputs and dismissal; repeated submissions are guarded synchronously. Password fields clear when the dialog closes or succeeds and never enter navigation state.

Login retains the existing local-account and required-password-change behavior. Labels are associated with their inputs, and the sign-in button names its pending state. The login view uses the existing RocketMQ image and shared theme tokens; illustrative throughput and health claims have been removed. No animation is required to operate sign-in or restore a session.

Authentication hooks prevent overlapping submissions and ignore login results after their view closes or another session becomes current. Password changes must still belong to the captured local sign-in. Signing out preserves the established behavior of clearing the local sign-in even when the service cannot acknowledge logout.

This change does not alter backend password policy, persisted session contracts or the default native window size. Windows DPI, minimum-window layouts, modal focus and the complete live authentication flow require browser/native acceptance in addition to automated checks.
