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

func tableAlicloudAlbLoadBalancer(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_alb_load_balancer",
		Description: "Alicloud Application Load Balancer",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("load_balancer_id"),
			Hydrate:    getAlbLoadBalancer,
			Tags:       map[string]string{"service": "alb", "action": "GetLoadBalancerAttribute"},
		},
		List: &plugin.ListConfig{
			Hydrate: listAlbLoadBalancers,
			Tags:    map[string]string{"service": "alb", "action": "ListLoadBalancers"},
			KeyColumns: []*plugin.KeyColumn{
				{Name: "load_balancer_name", Require: plugin.Optional},
				{Name: "load_balancer_status", Require: plugin.Optional},
				{Name: "address_type", Require: plugin.Optional},
				{Name: "vpc_id", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
				{Name: "address_ip_version", Require: plugin.Optional},
				{Name: "load_balancer_edition", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "load_balancer_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the ALB instance.",
			},
			{
				Name:        "load_balancer_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the ALB instance.",
			},
			{
				Name:        "load_balancer_status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the ALB instance. Valid values: Active, Provisioning, Configuring.",
			},
			{
				Name:        "load_balancer_edition",
				Type:        proto.ColumnType_STRING,
				Description: "The edition of the ALB instance. Valid values: Basic, Standard, StandardWithWaf.",
			},
			{
				Name:        "load_balancer_bussiness_status",
				Type:        proto.ColumnType_STRING,
				Description: "The business status of the ALB instance. Valid values: Normal, FinancialLocked.",
				Transform:   transform.FromField("LoadBalancerBussinessStatus"),
			},
			{
				Name:        "address_type",
				Type:        proto.ColumnType_STRING,
				Description: "The network type of the ALB instance. Valid values: Internet, Intranet.",
			},
			{
				Name:        "address_allocated_mode",
				Type:        proto.ColumnType_STRING,
				Description: "The mode used to assign IP addresses to zones of the ALB instance. Valid values: Fixed, Dynamic.",
			},
			{
				Name:        "address_ip_version",
				Type:        proto.ColumnType_STRING,
				Description: "The IP version. Valid values: IPv4, DualStack.",
			},
			{
				Name:        "ipv6_address_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of IPv6 address used by the ALB instance. Valid values: Internet, Intranet.",
			},
			{
				Name:        "dns_name",
				Type:        proto.ColumnType_STRING,
				Description: "The domain name of the ALB instance.",
				Transform:   transform.FromField("DNSName"),
			},
			{
				Name:        "vpc_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the virtual private cloud (VPC) to which the ALB instance belongs.",
			},
			{
				Name:        "resource_group_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the resource group.",
			},
			{
				Name:        "bandwidth_capacity",
				Type:        proto.ColumnType_INT,
				Description: "The maximum bandwidth of the ALB instance. Unit: Mbit/s.",
			},
			{
				Name:        "bandwidth_package_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the EIP bandwidth plan associated with the ALB instance.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the ALB instance was created.",
			},
			{
				Name:        "service_managed_enabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the ALB instance is managed by a service.",
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
			// Get hydrate columns
			{
				Name:        "zone_mappings",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getAlbLoadBalancer,
				Description: "The zones and vSwitches. You must specify at least two zones.",
			},
			{
				Name:        "access_log_config",
				Type:        proto.ColumnType_JSON,
				Description: "The configuration of access logs.",
			},
			{
				Name:        "deletion_protection_config",
				Type:        proto.ColumnType_JSON,
				Description: "The configuration of deletion protection.",
			},
			{
				Name:        "load_balancer_billing_config",
				Type:        proto.ColumnType_JSON,
				Description: "The billing configuration of the ALB instance.",
			},
			{
				Name:        "modification_protection_config",
				Type:        proto.ColumnType_JSON,
				Description: "The configuration of the configuration read-only mode.",
			},
			{
				Name:        "load_balancer_operation_locks",
				Type:        proto.ColumnType_JSON,
				Description: "The configuration of the operation lock.",
			},
			{
				Name:        "tags_src",
				Type:        proto.ColumnType_JSON,
				Description: "A list of tags attached to the ALB instance.",
				Transform:   transform.FromField("Tags"),
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Description: ColumnDescriptionTitle,
				Transform:   transform.FromField("LoadBalancerName"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionTags,
				Transform:   transform.From(albLoadBalancerTagMap),
			},
			{
				Name:        "akas",
				Type:        proto.ColumnType_JSON,
				Description: ColumnDescriptionAkas,
				Hydrate:     getAlbLoadBalancerAkas,
				Transform:   transform.FromValue(),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.From(albLoadBalancerRegion),
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

func listAlbLoadBalancers(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service connection
	client, err := ALBService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_alb_load_balancer.listAlbLoadBalancers", "connection_error", err)
		return nil, err
	}

	request := alb.CreateListLoadBalancersRequest()
	request.Scheme = "https"
	request.MaxResults = requests.NewInteger(50)

	if d.EqualsQualString("load_balancer_name") != "" {
		request.LoadBalancerNames = &[]string{d.EqualsQualString("load_balancer_name")}
	}
	if d.EqualsQualString("load_balancer_status") != "" {
		request.LoadBalancerStatus = d.EqualsQualString("load_balancer_status")
	}
	if d.EqualsQualString("address_type") != "" {
		request.AddressType = d.EqualsQualString("address_type")
	}
	if d.EqualsQualString("vpc_id") != "" {
		request.VpcIds = &[]string{d.EqualsQualString("vpc_id")}
	}
	if d.EqualsQualString("resource_group_id") != "" {
		request.ResourceGroupId = d.EqualsQualString("resource_group_id")
	}
	if d.EqualsQualString("address_ip_version") != "" {
		request.AddressIpVersion = d.EqualsQualString("address_ip_version")
	}
	if d.EqualsQualString("load_balancer_edition") != "" {
		request.LoadBalancerEditions = &[]string{d.EqualsQualString("load_balancer_edition")}
	}

	for {
		d.WaitForListRateLimit(ctx)

		var response *alb.ListLoadBalancersResponse

		b := retry.NewFibonacci(100 * time.Millisecond)
		err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
			var err error
			response, err = client.ListLoadBalancers(request)
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
			plugin.Logger(ctx).Error("alicloud_alb_load_balancer.listAlbLoadBalancers", "api_error", err, "request", request)
			return nil, err
		}

		for _, loadBalancer := range response.LoadBalancers {
			d.StreamListItem(ctx, loadBalancer)
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

func getAlbLoadBalancer(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	var id string
	if h.Item != nil {
		lb := h.Item.(alb.LoadBalancer)
		id = lb.LoadBalancerId
	} else {
		id = d.EqualsQuals["load_balancer_id"].GetStringValue()
	}

	// Empty check
	if id == "" {
		return nil, nil
	}

	// Create service connection
	client, err := ALBService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_alb_load_balancer.getAlbLoadBalancer", "connection_error", err)
		return nil, err
	}

	request := alb.CreateGetLoadBalancerAttributeRequest()
	request.Scheme = "https"
	request.LoadBalancerId = id

	var response *alb.GetLoadBalancerAttributeResponse

	b := retry.NewFibonacci(100 * time.Millisecond)
	err = retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
		var err error
		response, err = client.GetLoadBalancerAttribute(request)
		if err != nil {
			if serverErr, ok := err.(*errors.ServerError); ok {
				if serverErr.ErrorCode() == "Throttling" {
					return retry.RetryableError(err)
				}
			}
		}
		return err
	})

	if err != nil {
		plugin.Logger(ctx).Error("alicloud_alb_load_balancer.getAlbLoadBalancer", "api_error", err)
		return nil, err
	}

	return response, nil
}

func getAlbLoadBalancerAkas(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	var id string
	switch item := h.Item.(type) {
	case alb.LoadBalancer:
		id = item.LoadBalancerId
	case *alb.GetLoadBalancerAttributeResponse:
		id = item.LoadBalancerId
	}

	// Generate akas
	akas := []string{"acs:alb:" + region + ":" + accountID + ":loadbalancer/" + id}

	return akas, nil
}

//// TRANSFORM FUNCTIONS

func albLoadBalancerTagMap(_ context.Context, d *transform.TransformData) (interface{}, error) {
	var tags []alb.Tag
	switch item := d.HydrateItem.(type) {
	case alb.LoadBalancer:
		tags = item.Tags
	case *alb.GetLoadBalancerAttributeResponse:
		tags = item.Tags
	}

	if len(tags) == 0 {
		return nil, nil
	}

	turbotTagsMap := map[string]string{}
	for _, i := range tags {
		turbotTagsMap[i.Key] = i.Value
	}

	return turbotTagsMap, nil
}

func albLoadBalancerRegion(_ context.Context, d *transform.TransformData) (interface{}, error) {
	switch item := d.HydrateItem.(type) {
	case *alb.GetLoadBalancerAttributeResponse:
		return item.RegionId, nil
	}
	// For list items, use the matrix region
	region := d.MatrixItem[matrixKeyRegion]
	return region, nil
}
