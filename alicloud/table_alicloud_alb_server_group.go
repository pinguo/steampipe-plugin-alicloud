package alicloud

import (
	"context"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alb"
	"github.com/sethvargo/go-retry"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAlicloudAlbServerGroup(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_alb_server_group",
		Description: "Alicloud ALB Server Group",
		List: &plugin.ListConfig{
			Hydrate: listAlbServerGroups,
			Tags:    map[string]string{"service": "alb", "action": "ListServerGroups"},
			KeyColumns: []*plugin.KeyColumn{
				{Name: "server_group_id", Require: plugin.Optional},
				{Name: "server_group_name", Require: plugin.Optional},
				{Name: "server_group_type", Require: plugin.Optional},
				{Name: "vpc_id", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "server_group_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the server group.",
			},
			{
				Name:        "server_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the server group.",
			},
			{
				Name:        "server_group_status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the server group. Valid values: Creating, Available, Configuring.",
			},
			{
				Name:        "server_group_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the server group. Valid values: Instance, Ip, Fc.",
			},
			{
				Name:        "protocol",
				Type:        proto.ColumnType_STRING,
				Description: "The backend protocol. Valid values: HTTP, HTTPS, gRPC.",
			},
			{
				Name:        "vpc_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the VPC.",
			},
			{
				Name:        "scheduler",
				Type:        proto.ColumnType_STRING,
				Description: "The scheduling algorithm. Valid values: Wrr, Wlc, Sch.",
			},
			{
				Name:        "resource_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the resource group.",
			},
			{
				Name:        "server_count",
				Type:        proto.ColumnType_INT,
				Description: "The number of backend servers in the server group.",
			},
			{
				Name:        "service_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the service.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the server group was created.",
			},
			{
				Name:        "upstream_keepalive_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether persistent connections are enabled.",
			},
			{
				Name:        "ipv6_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether IPv6 is enabled.",
			},
			{
				Name:        "service_managed_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the server group is managed by a service.",
			},
			{
				Name:        "service_managed_mode",
				Type:        proto.ColumnType_STRING,
				Description: "The mode of service management.",
			},
			{
				Name:        "config_managed_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the configuration is managed.",
			},
			{
				Name:        "related_load_balancer_ids",
				Type:        proto.ColumnType_JSON,
				Description: "The IDs of the associated ALB instances.",
			},
			{
				Name:        "related_listener_ids",
				Type:        proto.ColumnType_JSON,
				Description: "The IDs of the associated listeners.",
			},
			{
				Name:        "related_rule_ids",
				Type:        proto.ColumnType_JSON,
				Description: "The IDs of the associated forwarding rules.",
			},
			{
				Name:        "health_check_config",
				Type:        proto.ColumnType_JSON,
				Description: "The health check configuration.",
			},
			{
				Name:        "sticky_session_config",
				Type:        proto.ColumnType_JSON,
				Description: "The session persistence configuration.",
			},
			{
				Name:        "uch_config",
				Type:        proto.ColumnType_JSON,
				Description: "The URL consistency hash configuration.",
			},
			{
				Name:        "connection_drain_config",
				Type:        proto.ColumnType_JSON,
				Description: "The connection drain configuration.",
			},
			{
				Name:        "slow_start_config",
				Type:        proto.ColumnType_JSON,
				Description: "The slow start configuration.",
			},
			{
				Name:        "tags_src",
				Type:        proto.ColumnType_JSON,
				Description: "A list of tags attached to the server group.",
				Transform:   transform.FromField("Tags"),
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Description: ColumnDescriptionTitle,
				Transform:   transform.FromField("ServerGroupName"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionTags,
				Transform:   transform.From(albServerGroupTagMap),
			},
			{
				Name:        "akas",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionAkas,
				Hydrate:     getAlbServerGroupAkas,
				Transform:   transform.FromValue(),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.From(albServerGroupRegion),
			},
			{
				Name:        "account_id",
				Description: ColumnDescriptionAccount,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCommonColumns,
				Transform:   transform.FromField("AccountID"),
			},
		},
	}
}

//// LIST FUNCTION

func listAlbServerGroups(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service connection
	client, err := ALBService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_alb_server_group.listAlbServerGroups", "connection_error", err)
		return nil, err
	}

	request := alb.CreateListServerGroupsRequest()
	request.Scheme = "https"
	request.MaxResults = requests.NewInteger(50)
	request.ShowRelationEnabled = requests.NewBoolean(true)

	if d.EqualsQualString("server_group_id") != "" {
		request.ServerGroupIds = &[]string{d.EqualsQualString("server_group_id")}
	}
	if d.EqualsQualString("server_group_name") != "" {
		request.ServerGroupNames = &[]string{d.EqualsQualString("server_group_name")}
	}
	if d.EqualsQualString("server_group_type") != "" {
		request.ServerGroupType = d.EqualsQualString("server_group_type")
	}
	if d.EqualsQualString("vpc_id") != "" {
		request.VpcId = d.EqualsQualString("vpc_id")
	}
	if d.EqualsQualString("resource_group_id") != "" {
		request.ResourceGroupId = d.EqualsQualString("resource_group_id")
	}

	for {
		d.WaitForListRateLimit(ctx)

		var response *alb.ListServerGroupsResponse

		b := retry.NewFibonacci(100 * time.Millisecond)
		err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
			var err error
			response, err = client.ListServerGroups(request)
			if err != nil {
				if serverErr, ok := err.(*errors.ServerError); ok {
					if serverErr.ErrorCode() == "Throttling" {
						return retry.RetryableError(err)
					}
				}
				return err
			}
			return nil
		})

		if err != nil {
			plugin.Logger(ctx).Error("alicloud_alb_server_group.listAlbServerGroups", "api_error", err, "request", request)
			return nil, err
		}

		for _, serverGroup := range response.ServerGroups {
			d.StreamListItem(ctx, serverGroup)
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if response.NextToken == "" {
			break
		}
		request.NextToken = response.NextToken
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getAlbServerGroupAkas(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	serverGroup := h.Item.(alb.ServerGroup)

	akas := []string{"acs:alb:" + region + ":" + accountID + ":servergroup/" + serverGroup.ServerGroupId}

	return akas, nil
}

//// TRANSFORM FUNCTIONS

func albServerGroupTagMap(_ context.Context, d *transform.TransformData) (interface{}, error) {
	serverGroup := d.HydrateItem.(alb.ServerGroup)

	if len(serverGroup.Tags) == 0 {
		return nil, nil
	}

	turbotTagsMap := map[string]string{}
	for _, i := range serverGroup.Tags {
		turbotTagsMap[i.Key] = i.Value
	}

	return turbotTagsMap, nil
}

func albServerGroupRegion(_ context.Context, d *transform.TransformData) (interface{}, error) {
	region := d.MatrixItem[matrixKeyRegion]
	return region, nil
}
