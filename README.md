# tree-terraform-provider

A set of packages that help you build a Terraform provider for resources organized into a tree.

The purpose of this repository is to simplify the development of future projects that seek to track hierarchical configuration in Terraform using a SQL-like storage mechanism. The "tree" feature of this is simply a bonus.

## Pitch

You maintain a hierarchy in YAML that records the arrangement and ownership of your organization, teams, products, and environments. This hierarchy is large and cumbersome to maintain, and if you break it there could be disastrous downstream effects.

Sure, you can add protections and helpers to your current configuration. You could break it up, develop a testing framework for your changes. These would add maintanence overhead, but everything requires new dependencies, right?

I would argue that a lot of the safeguards you want are already present in two dependencies you don't have to worry about so much: Go and Terraform. A custom terraform provider offers first-class testing capability through Go and the [provider plugin framework](https://developer.hashicorp.com/terraform/plugin/sdkv2/testing/acceptance-tests). You can develop very flexible validators for your data, and then you can unit-test those validators to ensure they do exactly what you need.

See /docs/rationale.md for more detail.

## Introduction

A small sample implementation is available in the `example/` directory. For a more complete implementation, see [spilliams/terraform-provider-tree-example](https://github.com/spilliams/terraform-provider-tree-example).

This helper uses DynamoDB as a storage mechanism for your provider's resources. I might add a sqlite3 plugin, but I have no plans to add other types of storage.
