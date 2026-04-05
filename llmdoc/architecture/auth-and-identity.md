# Auth and Identity

## Admin authorization modes
Admin access is granted when any of three conditions is true: `dev_open_admin`, ForwardAuth header match, or a valid built-in admin session cookie. Source: `src/server/state.rs:333`, `src/server/state.rs:337`, `src/server/state.rs:340`.

## ForwardAuth model
- `ForwardAuthConfig` is configured by header names and an expected admin identity value. Source: `src/server/state.rs:14`, `src/server/state.rs:81`.
- A request is considered admin only when the configured user header value exactly matches `admin_value`. Source: `src/server/state.rs:148`.
- Profile and maintenance actor resolution can also reuse ForwardAuth nickname or user identity for display. Source: `src/server/handlers/admin_auth.rs:296`, `src/server/state.rs:386`.

## Built-in admin login
- Built-in admin mode uses the `hikari_admin_session` HttpOnly cookie and in-memory session storage. Source: `src/server/state.rs:160`, `src/server/state.rs:175`.
- Password verification prefers Argon2 PHC hashes and can fall back to a plaintext configured password when hash is absent. Source: `src/server/state.rs:213`, `src/server/state.rs:217`.
- Session storage is bounded and evicts oldest sessions past the configured cap. Source: `src/server/state.rs:234`, `src/server/state.rs:250`.
- Login/logout APIs live at `/api/admin/login` and `/api/admin/logout`. Source: `src/server/handlers/admin_auth.rs:502`, `src/server/handlers/admin_auth.rs:524`.

## End-user identity and Linux DO OAuth
- End-user auth is separate from admin auth and depends on Linux DO OAuth plus a `hikari_user_session` cookie. Source: `src/server/state.rs:163`, `src/server/state.rs:346`.
- OAuth login creates a one-time state token plus a binding cookie to protect the callback flow and optionally preserve a preferred token binding. Source: `src/server/handlers/user.rs:76`, `src/server/handlers/user.rs:78`, `src/server/handlers/user.rs:167`.
- Successful callback upserts the OAuth account, ensures a user-token binding, creates a user session, and redirects into `/console` when available. Source: `src/server/handlers/user.rs:303`, `src/server/handlers/user.rs:316`, `src/server/handlers/user.rs:329`, `src/server/handlers/user.rs:341`.

## Secure cookie behavior
Cookie `Secure` behavior is inferred from reverse-proxy headers (`X-Forwarded-Proto` or RFC7239 `Forwarded`). Source: `src/server/state.rs:304`.
