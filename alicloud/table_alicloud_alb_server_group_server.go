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

func tableAlicloudAlbServerGroupServer(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_alb_server_group_server",
		Description: "Alicloud ALB Server Group Server (Backend Server)",
		List: &plugin.ListConfig{
			ParentHydrate: listAlbServerGroups,
			Hydrate:       listAlbServerGroupServers,
			Tags:          map[string]string{"service": "alb", "action": "ListServerGroupServers"},
			KeyColumns: []*plugin.KeyColumn{
				{Name: "server_group_id", Require: plugin.Optional},
				{Name: "server_id", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "server_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the backend server.",
			},
			{
				Name:        "server_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the server group.",
			},
			{
				Name:        "server_ip",
				Type:        proto.ColumnType_IPADDR,
				Description: "The IP address of the backend server.",
			},
			{
				Name:        "server_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the backend server. Valid values: Ecs, Eni, Eci, Ip, Fc.",
			},
			{
				Name:        "port",
				Type:        proto.ColumnType_INT,
				Description: "The port used by the backend server.",
			},
			{
				Name:        "weight",
				Type:        proto.ColumnType_INT,
				Description: "The weight of the backend server.",
			},
			{
				Name:        "status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the backend server. Valid values: Adding, Available, Configuring, Removing.",
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "The description of the backend server.",
			},
			{
				Name:        "remote_ip_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the remote IP address feature is enabled.",
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Description: ColumnDescriptionTitle,
				Transform:   transform.FromField("ServerId"),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.From(albServerGroupServerRegion),
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

func listAlbServerGroupServers(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	// Get server group ID from parent hydrate
	var serverGroupId string
	if h.Item != nil {
		serverGroup := h.Item.(alb.ServerGroup)
		serverGroupId = serverGroup.ServerGroupId
	}

	// If a specific server_group_id is provided in the query, use it
	if d.EqualsQualString("server_group_id") != "" {
		serverGroupId = d.EqualsQualString("server_group_id")
	}

	if serverGroupId == "" {
		return nil, nil
	}

	// If parent hydrate returned a different server group than what's filtered, skip
	if d.EqualsQualString("server_group_id") != "" && serverGroupId != d.EqualsQualString("server_group_id") {
		return nil, nil
	}

	// Create service connection
	client, err := ALBService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_alb_server_group_server.listAlbServerGroupServers", "connection_error", err)
		return nil, err
	}

	request := alb.CreateListServerGroupServersRequest()
	request.Scheme = "https"
	request.ServerGroupId = serverGroupId
	request.MaxResults = requests.NewInteger(100)

	if d.EqualsQualString("server_id") != "" {
		request.ServerId = d.EqualsQualString("server_id")
	}

	for {
		d.WaitForListRateLimit(ctx)

		var response *alb.ListServerGroupServersResponse

		b := retry.NewFibonacci(100 * time.Millisecond)
		err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
			var err error
			response, err = client.ListServerGroupServers(request)
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
			plugin.Logger(ctx).Error("alicloud_alb_server_group_server.listAlbServerGroupServers", "api_error", err, "request", request)
			return nil, err
		}

		for _, server := range response.Servers {
			d.StreamListItem(ctx, server)
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

//// TRANSFORM FUNCTIONS

func albServerGroupServerRegion(_ context.Context, d *transform.TransformData) (interface{}, error) {
	region := d.MatrixItem[matrixKeyRegion]
	return region, nil
}
