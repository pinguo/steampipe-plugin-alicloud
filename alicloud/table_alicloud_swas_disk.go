package alicloud

import (
	"context"
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

func tableAlicloudSwasDisk(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_swas_disk",
		Description: "Alicloud Simple Application Server (SWAS) Disk",
		List: &plugin.ListConfig{
			Hydrate: listSwasDisks,
			Tags:    map[string]string{"service": "swas-open", "action": "ListDisks"},
			KeyColumns: []*plugin.KeyColumn{
				{Name: "disk_id", Require: plugin.Optional},
				{Name: "instance_id", Require: plugin.Optional},
				{Name: "disk_type", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "disk_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the disk.",
			},
			{
				Name:        "disk_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the disk.",
			},
			{
				Name:        "status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the disk. Valid values: ReIniting, Creating, Available, In_use, Attaching, Detaching.",
			},
			{
				Name:        "disk_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the disk. Valid values: System, Data.",
			},
			{
				Name:        "category",
				Type:        proto.ColumnType_STRING,
				Description: "The category of the disk. Valid values: cloud_efficiency, cloud_ssd.",
			},
			{
				Name:        "size",
				Type:        proto.ColumnType_INT,
				Description: "The size of the disk. Unit: GB.",
			},
			{
				Name:        "instance_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the simple application server to which the disk is attached.",
			},
			{
				Name:        "instance_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the simple application server to which the disk is attached.",
			},
			{
				Name:        "device",
				Type:        proto.ColumnType_STRING,
				Description: "The device name of the disk on the instance.",
			},
			{
				Name:        "disk_charge_type",
				Type:        proto.ColumnType_STRING,
				Description: "The billing method of the disk.",
			},
			{
				Name:        "creation_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the disk was created.",
			},
			{
				Name:        "resource_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the resource group.",
			},
			{
				Name:        "remark",
				Type:        proto.ColumnType_STRING,
				Description: "The remarks of the disk.",
			},
			{
				Name:        "tags_src",
				Type:        proto.ColumnType_JSON,
				Description: "A list of tags attached to the disk.",
				Transform:   transform.FromField("Tags"),
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Description: ColumnDescriptionTitle,
				Transform:   transform.From(swasDiskTitle),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionTags,
				Transform:   transform.From(swasDiskTagMap),
			},
			{
				Name:        "akas",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionAkas,
				Hydrate:     getSwasDiskAkas,
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

func listSwasDisks(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service connection
	client, err := SWASService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_swas_disk.listSwasDisks", "connection_error", err)
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

	request := swas.CreateListDisksRequest()
	request.Scheme = "https"
	request.PageSize = requests.NewInteger(int(maxLimit))
	request.PageNumber = requests.NewInteger(1)

	if d.EqualsQualString("disk_id") != "" {
		request.DiskIds = d.EqualsQualString("disk_id")
	}
	if d.EqualsQualString("instance_id") != "" {
		request.InstanceId = d.EqualsQualString("instance_id")
	}
	if d.EqualsQualString("disk_type") != "" {
		request.DiskType = d.EqualsQualString("disk_type")
	}
	if d.EqualsQualString("resource_group_id") != "" {
		request.ResourceGroupId = d.EqualsQualString("resource_group_id")
	}

	count := 0
	for {
		d.WaitForListRateLimit(ctx)

		var response *swas.ListDisksResponse

		b := retry.NewFibonacci(100 * time.Millisecond)
		err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
			var err error
			response, err = client.ListDisks(request)
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
			plugin.Logger(ctx).Error("alicloud_swas_disk.listSwasDisks", "api_error", err, "request", request)
			return nil, err
		}

		for _, disk := range response.Disks {
			d.StreamListItem(ctx, disk)
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

func getSwasDiskAkas(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	disk := h.Item.(swas.Disk)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	akas := []string{"acs:swas-open:" + disk.RegionId + ":" + accountID + ":disk/" + disk.DiskId}

	return akas, nil
}

//// TRANSFORM FUNCTIONS

func swasDiskTitle(_ context.Context, d *transform.TransformData) (interface{}, error) {
	disk := d.HydrateItem.(swas.Disk)
	if disk.DiskName != "" {
		return disk.DiskName, nil
	}
	return disk.DiskId, nil
}

func swasDiskTagMap(_ context.Context, d *transform.TransformData) (interface{}, error) {
	disk := d.HydrateItem.(swas.Disk)

	if len(disk.Tags) == 0 {
		return nil, nil
	}

	turbotTagsMap := map[string]string{}
	for _, i := range disk.Tags {
		turbotTagsMap[i.Key] = i.Value
	}

	return turbotTagsMap, nil
}
