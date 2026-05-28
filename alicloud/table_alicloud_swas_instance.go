package alicloud

import (
	"context"
	"fmt"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	swas "github.com/aliyun/alibaba-cloud-sdk-go/services/swas-open"
	"github.com/sethvargo/go-retry"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAlicloudSwasInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_swas_instance",
		Description: "Alicloud Simple Application Server (SWAS) Instance",
		List: &plugin.ListConfig{
			Hydrate: listSwasInstances,
			Tags:    map[string]string{"service": "swas-open", "action": "ListInstances"},
			KeyColumns: []*plugin.KeyColumn{
				{Name: "instance_id", Require: plugin.Optional},
				{Name: "instance_name", Require: plugin.Optional},
				{Name: "status", Require: plugin.Optional},
				{Name: "charge_type", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
				{Name: "public_ip_address", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "instance_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the simple application server.",
			},
			{
				Name:        "instance_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the simple application server.",
			},
			{
				Name:        "status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the instance. Valid values: Pending, Starting, Running, Stopping, Stopped, Resetting, Upgrading, Disabled.",
			},
			{
				Name:        "charge_type",
				Type:        proto.ColumnType_STRING,
				Description: "The billing method of the instance.",
			},
			{
				Name:        "business_status",
				Type:        proto.ColumnType_STRING,
				Description: "The business status of the instance.",
			},
			{
				Name:        "plan_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the plan.",
			},
			{
				Name:        "image_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the image.",
			},
			{
				Name:        "public_ip_address",
				Type:        proto.ColumnType_IPADDR,
				Description: "The public IP address of the instance.",
			},
			{
				Name:        "inner_ip_address",
				Type:        proto.ColumnType_IPADDR,
				Description: "The internal IP address of the instance.",
			},
			{
				Name:        "creation_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the instance was created.",
			},
			{
				Name:        "expired_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the instance expires.",
			},
			{
				Name:        "ddos_status",
				Type:        proto.ColumnType_STRING,
				Description: "The DDoS protection status of the instance.",
			},
			{
				Name:        "disable_reason",
				Type:        proto.ColumnType_STRING,
				Description: "The reason why the instance is disabled.",
			},
			{
				Name:        "combination",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the instance is a combination instance.",
			},
			{
				Name:        "combination_instance_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the combination instance.",
			},
			{
				Name:        "uuid",
				Type:        proto.ColumnType_STRING,
				Description: "The UUID of the instance.",
			},
			{
				Name:        "resource_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the resource group.",
			},
			{
				Name:        "resource_spec",
				Type:        proto.ColumnType_JSON,
				Description: "The resource specification of the instance, including CPU, memory, disk, and bandwidth.",
			},
			{
				Name:        "image",
				Type:        proto.ColumnType_JSON,
				Description: "The image information of the instance.",
			},
			{
				Name:        "disks",
				Type:        proto.ColumnType_JSON,
				Description: "The disks attached to the instance.",
			},
			{
				Name:        "tags_src",
				Type:        proto.ColumnType_JSON,
				Description: "A list of tags attached to the instance.",
				Transform:   transform.FromField("Tags"),
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Description: ColumnDescriptionTitle,
				Transform:   transform.FromField("InstanceName"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionTags,
				Transform:   transform.From(swasInstanceTagMap),
			},
			{
				Name:        "akas",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionAkas,
				Hydrate:     getSwasInstanceAkas,
				Transform:   transform.FromValue(),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RegionId"),
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

func listSwasInstances(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service connection
	client, err := SWASService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_swas_instance.listSwasInstances", "connection_error", err)
		return nil, err
	}

	// Limiting the results
	maxLimit := int32(100)
	if d.QueryContext.Limit != nil {
		limit := int32(*d.QueryContext.Limit)
		if limit < maxLimit {
			maxLimit = limit
		}
	}

	request := swas.CreateListInstancesRequest()
	request.Scheme = "https"
	request.Domain = fmt.Sprintf("swas.%s.aliyuncs.com", d.EqualsQualString(matrixKeyRegion))
	request.PageSize = requests.NewInteger(int(maxLimit))
	request.PageNumber = requests.NewInteger(1)

	if d.EqualsQualString("instance_name") != "" {
		request.InstanceName = d.EqualsQualString("instance_name")
	}
	if d.EqualsQualString("instance_id") != "" {
		request.InstanceIds = d.EqualsQualString("instance_id")
	}
	if d.EqualsQualString("status") != "" {
		request.Status = d.EqualsQualString("status")
	}
	if d.EqualsQualString("charge_type") != "" {
		request.ChargeType = d.EqualsQualString("charge_type")
	}
	if d.EqualsQualString("resource_group_id") != "" {
		request.ResourceGroupId = d.EqualsQualString("resource_group_id")
	}
	if d.EqualsQualString("public_ip_address") != "" {
		request.PublicIpAddresses = d.EqualsQualString("public_ip_address")
	}

	count := 0
	for {
		d.WaitForListRateLimit(ctx)

		var response *swas.ListInstancesResponse

		b := retry.NewFibonacci(100 * time.Millisecond)
		err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
			var err error
			response, err = client.ListInstances(request)
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
			plugin.Logger(ctx).Error("alicloud_swas_instance.listSwasInstances", "api_error", err, "request", request)
			return nil, err
		}

		for _, instance := range response.Instances {
			d.StreamListItem(ctx, instance)
			count++
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if count >= response.TotalCount {
			break
		}
		request.PageNumber = requests.NewInteger(response.PageNumber + 1)
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getSwasInstanceAkas(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	instance := h.Item.(swas.Instance)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	akas := []string{"acs:swas-open:" + instance.RegionId + ":" + accountID + ":instance/" + instance.InstanceId}

	return akas, nil
}

//// TRANSFORM FUNCTIONS

func swasInstanceTagMap(_ context.Context, d *transform.TransformData) (interface{}, error) {
	instance := d.HydrateItem.(swas.Instance)

	if len(instance.Tags) == 0 {
		return nil, nil
	}

	turbotTagsMap := map[string]string{}
	for _, i := range instance.Tags {
		turbotTagsMap[i.Key] = i.Value
	}

	return turbotTagsMap, nil
}
