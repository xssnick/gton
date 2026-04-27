# flexserver style guide

## Reference implementation

- `cppnode` contains the C++ node implementation.
- Use it for research and to verify that our behavior matches the reference implementation.

## Architecture

- `main` is the composition root.
- Initialize flags, logger, storage, node, service, and other dependencies in `main`.
- Do not make services assemble the application graph inside themselves.

- `service` owns the workflow it is responsible for.
- If a service processes blocks, it should control the full processing loop itself.
- `main` should only initialize services and handle console commands or process lifecycle.

- Do not export pass-through methods from services.
- If a method only forwards a call to a lower layer without adding meaning or policy, remove it.

- Do not make reverse compatibility when changing some code or architecture. We are in an active development phase and can drop old data.

## Package structure

- Split code by domain responsibility.
- Create packages only for stable, meaningful domains.
- Do not create tiny packages for incidental helpers or one-off wrappers.

- Avoid packages and names like `bridge`, `adapter`, `converter`, or similar unless there is a real boundary that requires them.
- Prefer direct code over glue layers.

## API design

- Prefer linear APIs.
- Do not use tri-state returns like `(value, ok, err)` in storage and domain code.

- When data may be absent, use `(value, error)` and return a dedicated not-found error such as `ErrNotFound`.

- If `err == nil`, the returned value must already be valid and ready to use.
- Do not design APIs where `err == nil` but the caller still has to inspect `ok` or check the value for `nil`.

- Keep boolean returns only when they represent a real property or business flag, not presence or absence of data.

- Do not add wrapper APIs, aliases, or renames that do not simplify the code.
- Avoid patterns like:
  - `type X = Y`
  - `type Options = ImplOptions`
  - `Open -> OpenImpl`
- Name public types and functions correctly once and use them directly.

- Do not introduce internal conversion helpers like `fromX` and `toX` unless there is a real format boundary, protocol boundary, or external API boundary.
- Inside the project, prefer using the actual types directly.

## Code style

- Add empty lines between logical blocks inside functions.

- Do not make excessive nil checks.
- Check only what can really be nil logically.
- Do not check obvious value inputs for nil.

- Keep code simple and optimized.
- Add abstractions only when they remove real duplication or represent a real boundary.

- Prefer straightforward Go-style code.
- Keep the control flow linear and easy to read.
- Avoid unnecessary indirection.

## Protocol types

- Any type used in `tl.Register(...)` must be public and named with a capital letter.

## Tests

- Keep test-only constants, hooks, and helpers in `_test.go` files.
- Do not leave test-only code in normal production files.

## Logging

- Use `zerolog` for runtime logging.
- Use log levels intentionally: `debug`, `info`, `warn`, `error`.
- Do not use ad hoc `fmt.Println` style logging in runtime code.
- Log formatting should be configurable from startup, with both pretty console output and JSON output supported.
