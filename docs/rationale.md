# Rationale

Imagine this scenario. You have a tree hierarchy in YAML:

```yaml
organizations:
    acme:
        teams:
            product:
                environments:
                    development:
                        ...
                    staging:
                        ...
                    production:
                        ...
            marketing:
                ...
            skunkworks:
                ...
```

This YAML is the basis of your operations model, so changing it is risky. You want to improve this system by encoding this information another way, with the safeguards that IaC provides. Something like

```hcl
resource "tree_node" "acme" {
    type = "organization"
    name = "acme"
}
resource "tree_node" "product_team" {
    type = "team"
    parent = tree_node.acme.id
    name = "product"
}
resource "tree_node" "product_dev" {
    type = "environment"
    parent = tree_node.product_team.id
    name = "development"
}
resource "tree_node" "product_stage" {
    type = "environment"
    parent = tree_node.product_team.id
    name = "staging"
}
resource "tree_node" "product_prod" {
    type = "environment"
    parent = tree_node.product_team.id
    name = "production"
}
resource "tree_node" "marketing_team" {
    type = "team"
    parent = tree_node.acme.id
    name = "marketing"
}
resource "tree_node" "skunkworks_team" {
    type = "team"
    parent = tree_node.acme.id
    name = "skunkworks"
}
```

What have we gained by this?

## Readability

Changing from 15 lines of YAML to 34 lines of HCL is less readable, until you consider that the HCL can be split into multiple files for clarity.

Sure, you could also split the YAML into multiple files for the same clarity, but then you might need to merge the files back together somehow. Maintaining a dependency to merge yaml could be just as complex as maintaining a dependency on Terraform (and now I'm in the weeds).

## Validations

You can add validation to your yaml using something like [JSON Schema](https://json-schema-everywhere.github.io/yaml), which works in many IDEs and can be operated in CI/CD environments using something like [pre-commit](https://github.com/python-jsonschema/check-jsonschema/blob/main/README.md).

The flexibility offered by Go to form complex validators is probably a better developer experience than contorting another ocnfiguration language into something resembling logic. For instance: if each of my environments is assigned an IPv4 CIDR, I might want a validator that ensures none of those CIDRs overlap.

Unit-testing validators in Go is straightforward. It looks like there is at least one [testing framework for json schema](https://github.com/json-schema-org/JSON-Schema-Test-Suite), but I'd rather use language-native concepts than add another dependency.

## WORM

(write once read many)

This information is sacred to your operations. You rely on it for accuracy in building your engineering platform. Maybe you could also _publish_ this information though, for your internal stakeholders to use.

A terraform provider offers a `data_source` feature to read resources without the risk of changing them.
