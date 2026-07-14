# Security Policy

## Supported versions

This project is pre-1.0. Security fixes are applied to the latest released
minor version only; there are no long-term support branches yet.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for a
suspected vulnerability.

Use GitHub's private vulnerability reporting: on the repository's **Security**
tab, choose **Report a vulnerability**. This opens a private advisory visible
only to the maintainer.

When reporting, include as much of the following as you can:

- affected module and version (core library and/or the Valkey backend);
- a description of the issue and its impact;
- steps to reproduce, ideally a minimal example;
- any known mitigations.

You can expect an initial acknowledgement within a few days. Once a fix is
ready it will be released as a new patch version and noted in
[CHANGELOG.md](CHANGELOG.md), with credit to the reporter unless anonymity is
requested.
