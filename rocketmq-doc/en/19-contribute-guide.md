---
title: "Contribute Guide"
permalink: /docs/contribute-guide/
excerpt: "How to contribute to RocketMQ Rust."
last_modified_at: 2025-01-30T22:48:05-23:00:05
redirect_from:
  - /theme-setup/
toc: false
classes: wide
---

You can report a bug, submit a new function enhancement suggestion, or submit a pull request directly.

## Reporting Issues

- Before submitting an issue, please go through a comprehensive search to make sure the problem cannot be solved just by
  searching.
- Check the [Issue List](https://github.com/mxsm/rocketmq-rust/issues) to make sure the problem is not repeated.
- Create a new issue and choose the type of issue.
- Define the issue with a clear and descriptive title.
- Fill in necessary information according to the template.
- Please pay attention for your issue, you may need provide more information during discussion.

## How to Contribute

#### 1. Prepare repository

Go to [Rocketmq Rust GitHub Repo](https://github.com/mxsm/rocketmq-rust) and fork repository to your account.

Clone repository to local machine.

```bash
https://github.com/(github name)/rocketmq-rust.git
```

Add **`Rocketmq-rust`** remote repository.

```shell
$ cd rocketmq-rust
$ git remote add mxsm https://github.com/mxsm/rocketmq-rust.git
$ git remote -v
$ git fetch mxsm
```

#### 2. Choose Issue

Please choose the issue to be edited. If it is a new issue discovered or a new function enhancement to offer, please
create an issue and set the right label for it.

#### 3. Create Branch

```shell
$ git checkout main
$ git fetch mxsm
$ git rebase mxsm/main
$ git checkout -b feature-issueNo(custom)
```

**Note:** We will merge PR using squash, commit log will be different with upstream if you use old branch.
{: .notice--warning}

#### 4. Coding

After the development is completed, it is necessary to perform code formatting, compilation, and format checking.

**Format the code in the project**

```shell
cargo fmt --all
```

**Build**

```shell
cargo build 
```

**Run Clippy**

```
cargo clippy --all-targets --all-features --workspace

```

**Run all tests:**

```
cargo test --all-features --workspace
```

**push code to your fork repo**

```
git add modified-file-names
git commit -m 'commit log'
git push origin feature-issueNo(custom)
```

**Submit Pull Request**

- Send a pull request to the main branch.
- The mentor will do code review before discussing some details (including the design, the implementation and the
  performance) with you. The request will be merged into the branch of current development version after the edit is
  well enough.
- At last, congratulations on being a contributor of rocketmq-rust

**Note:** 🚨The code review suggestions from CodeRabbit are to be used as a reference only, and the PR submitter can
decide whether to make changes based on their own judgment. Ultimately, the project management personnel will conduct
the final code review💥
{: .notice--info}