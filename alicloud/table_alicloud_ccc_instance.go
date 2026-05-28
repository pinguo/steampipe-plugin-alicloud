package alicloud

import (
	"context"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ccc"
)

func tableAlicloudCccInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_ccc_instance",
		Description: "Alicloud Cloud Call Center (CCC) Instance",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("instance_id"),
			Hydrate:    getCccInstance,
			Tags:       map[string]string{"service": "ccc", "action": "GetInstance"},
		},
		List: &plugin.ListConfig{
			Hydrate: listCccInstances,
			Tags:    map[string]string{"service": "ccc", "action": "ListInstances"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The name of the CCC instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_id",
				Description: "The ID of the CCC instance.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id"),
			},
			{
				Name:        "status",
				Description: "The status of the CCC instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "domain_name",
				Description: "The domain name of the CCC instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "console_url",
				Description: "The console URL of the CCC instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "The description of the CCC instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "aliyun_uid",
				Description: "The Alibaba Cloud UID of the instance owner.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "create_time",
				Description: "The time when the instance was created (Unix timestamp in milliseconds).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "number_list",
				Description: "The list of phone numbers associated with the instance.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "admin_list",
				Description: "The list of administrators for the instance.",
				Type:        proto.ColumnType_JSON,
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCccInstanceAkas,
				Transform:   transform.FromValue(),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCccInstanceRegion,
				Transform:   transform.FromValue(),
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

func listCccInstances(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	client, err := CCCService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_ccc_instance.listCccInstances", "connection_error", err)
		return nil, err
	}

	request := ccc.CreateListInstancesRequest()
	request.Scheme = "https"
	request.PageSize = requests.NewInteger(100)
	request.PageNumber = requests.NewInteger(1)

	// If the requested number of items is less than the paging max limit
	limit := d.QueryContext.Limit
	if limit != nil {
		if *limit < 100 {
			request.PageSize = requests.NewInteger(int(*limit))
		}
	}

	for {
		d.WaitForListRateLimit(ctx)

		response, err := client.ListInstances(request)
		if err != nil {
			plugin.Logger(ctx).Error("alicloud_ccc_instance.listCccInstances", "query_error", err, "request", request)
			return nil, err
		}

		for _, instance := range response.Data.List {
			d.StreamListItem(ctx, instance)
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if len(response.Data.List) < response.Data.PageSize {
			break
		}
		request.PageNumber = requests.NewInteger(response.Data.PageNumber + 1)
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCccInstance(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	client, err := CCCService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_ccc_instance.getCccInstance", "connection_error", err)
		return nil, err
	}

	var id string
	if h.Item != nil {
		instance := h.Item.(ccc.CallCenterInstance)
		id = instance.Id
	} else {
		id = d.EqualsQuals["instance_id"].GetStringValue()
	}

	request := ccc.CreateGetInstanceRequest()
	request.Scheme = "https"
	request.InstanceId = id

	response, err := client.GetInstance(request)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_ccc_instance.getCccInstance", "query_error", err, "request", request)
		return nil, err
	}

	if response.Data.Id != "" {
		return response.Data, nil
	}

	return nil, nil
}

func getCccInstanceAkas(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	instance := h.Item.(ccc.CallCenterInstance)

	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	arn := "acs:ccc::" + accountID + ":instance/" + instance.Id
	return []string{arn}, nil
}

func getCccInstanceRegion(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)
	if region == "" {
		region = GetDefaultRegion(d.Connection)
	}
	return region, nil
}
