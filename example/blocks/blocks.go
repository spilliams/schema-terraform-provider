package blocks

import (
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/resource"
)

func AllDataSources() []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

func AllResources() []func() resource.Resource {
	return []func() resource.Resource{}
}
