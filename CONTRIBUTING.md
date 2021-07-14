# Contributing to AtomicDEX Marketmaker

We welcome contribution from everyone in the form of suggestions, bug reports, pull requests, and feedback.
Please note we have a code of conduct, please follow it in all your interactions with the project.

## Submitting feature requests

Before uploading any changes, please make sure that the test suite passes locally before submitting a pull request with your changes.

```
cargo test --all
```

We also use [Clippy](https://github.com/rust-lang/rust-clippy) to avoid common mistakes
and we use [rustfmt](https://github.com/rust-lang/rustfmt) to make our code clear to everyone.

1. Install these tools (only once):
    ```
    rustup component add rustfmt --toolchain nightly-2020-02-01
    rustup component add clippy
    ```
1. Format the code using rustfmt:
    ```
    cargo +nightly fmt
    ```
1. Make sure there are no warnings and errors. Run the Clippy:
    ```
    cargo clippy -- -D warnings
    ```

### Run WASM tests

1. Install Firefox.
1. Download Gecko driver for your OS: https://github.com/mozilla/geckodriver/releases
1. Run the tests
    ```
    WASM_BINDGEN_TEST_TIMEOUT=120 GECKODRIVER=PATH_TO_GECKO_DRIVER_BIN wasm-pack test --firefox --headless
    ```
