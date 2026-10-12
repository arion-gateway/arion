# Contributing to Arion

Thank you for your interest in Arion. The project is maintained by a small core team that designs, codes and reviews everything in Arion. In this age of AI and agentic coding, a lot has changed in software development. This guide will try to explain a little about how we work and how best to contribute to the project.

## Start with a conversation

The contributions that shape Arion most are discussions, bug reports and feature requests:

- **Questions and ideas** belong on [GitHub Discussions](https://github.com/arion-gateway/arion/discussions). Public answers become documentation for the next person.
- **Bugs** belong on [GitHub issues](https://github.com/arion-gateway/arion/issues). Include the Arion version, the smallest configuration that reproduces the problem, the expected and actual behaviour, and sanitised logs.
- **Feature requests** work best as problems, not designs: describe the requirement and the constraints in an issue or discussion. We would rather design the solution together than review one that arrives fully formed.

## Before opening a pull request

**Open a discussion or an issue first, before writing code.** We are deliberate about what lands on the data path and our review bandwidth is limited, so we prefer to capture the requirement in the open, agree on a design, and often the core team will prefer to implement it ourselves by including it in upcoming work. An unannounced pull request is almost certain to be refused.

It is also important to note that, while Arion has a very broad set of features, the core team are conservative about adding new features to the system. It is very easy for a system to become bloated and less useful for everyone by adding lots of divergent features that only satisify specific niche cases. And so, the Arion core team will often refuse or reject adding extensions that are not likely to be generally useful to a wide set of users.

Small, obvious fixes are the exception and are always welcome as direct pull requests: typos, documentation corrections and fixes for clearly broken behaviour.

## Security issues

Never report a suspected vulnerability in a public issue, discussion or pull request. Use [GitHub private vulnerability reporting](https://github.com/arion-gateway/arion/security/advisories/new) instead — [SECURITY.md](SECURITY.md) describes the process.

## License

Arion is licensed under [Apache-2.0](LICENSE). By contributing, you agree that your contributions are licensed under the same terms.
