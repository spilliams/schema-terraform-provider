package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/spilliams/tree-terraform-provider/example/blocks"
	"github.com/spilliams/tree-terraform-provider/pkg/storage/dynamodb"
)

const (
	providerAttrAWSProfile = "profile"
	providerAttrAWSRegion  = "region"
	providerAttrTableName  = "table_name"
	providerAttrKeyARN     = "kms_key_arn"
)

type treeProviderModel struct {
	AWSProfile types.String `tfsdk:"profile"`
	AWSRegion  types.String `tfsdk:"region"`
	TableName  types.String `tfsdk:"table_name"`
	KMSKeyARN  types.String `tfsdk:"kms_key_arn"`
}

type treeProvider struct {
	version string
	commit  string
}

var _ provider.Provider = &treeProvider{}

func New(version, commit string) func() provider.Provider {
	return func() provider.Provider {
		return &treeProvider{version, commit}
	}
}

func (tree *treeProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "tree"
	resp.Version = fmt.Sprintf("%s-%s", tree.version, tree.commit)
}

func (tree *treeProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Interact with the information architecture of the engineering platform.",
		Attributes: map[string]schema.Attribute{
			providerAttrAWSProfile: schema.StringAttribute{
				Description: "The AWS profile to use for DynamoDB storage.",
				Required:    true,
			},
			providerAttrAWSRegion: schema.StringAttribute{
				Description: "The AWS region to use for DynamoDB storage.",
				Required:    true,
			},
			providerAttrTableName: schema.StringAttribute{
				Description: "The table name to use for DynamoDB storage.",
				Required:    true,
			},
			providerAttrKeyARN: schema.StringAttribute{
				Description: "The ARN of the KMS key to use for encrypting the DynamoDB storage.",
				Required:    true,
			},
		},
	}
}

func (tree *treeProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config treeProviderModel
	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if config.AWSProfile.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root(providerAttrAWSProfile),
			"Unknown profile",
			"Cannot configure the provider client with an unknown profile.",
		)
	}
	if config.AWSRegion.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root(providerAttrAWSRegion),
			"Unknown region",
			"Cannot configure the provider client with an unknown region.",
		)
	}
	ctx = tflog.SetField(ctx, providerAttrAWSRegion, config.AWSRegion.ValueString())
	if config.TableName.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root(providerAttrTableName),
			"Unknown table name",
			"Cannot configure the provider client with an unknown DynamoDB storage table name.",
		)
	}
	ctx = tflog.SetField(ctx, providerAttrTableName, config.TableName.ValueString())
	if config.KMSKeyARN.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root(providerAttrKeyARN),
			"Unknown KMS Key ARN",
			"Cannot configure the provider client with an unknown KMS Key ARN.",
		)
	}
	if resp.Diagnostics.HasError() {
		return
	}

	client, err := dynamodb.NewClient(ctx,
		config.AWSProfile.ValueString(),
		config.AWSRegion.ValueString(),
		config.TableName.ValueString(),
		config.KMSKeyARN.ValueString(),
	)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to create provider client",
			"An unexpected error occurred when creating the provider client.\n\n"+
				err.Error(),
		)
	}
	if resp.Diagnostics.HasError() {
		return
	}
	resp.DataSourceData = client
	resp.ResourceData = client
}

func (tree *treeProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return blocks.AllDataSources()
}

func (tree *treeProvider) Resources(_ context.Context) []func() resource.Resource {
	return blocks.AllResources()
}
