package alicloud

import (
	"context"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/alikafka"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAlicloudKafkaInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_kafka_instance",
		Description: "Alicloud Kafka Instance",
		List: &plugin.ListConfig{
			Hydrate: listKafkaInstances,
			Tags:    map[string]string{"service": "alikafka", "action": "GetInstanceList"},
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "instance_id", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
				{Name: "order_id", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			// Top columns
			{
				Name:        "instance_id",
				Description: "The ID of the Kafka instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "name",
				Description: "The name of the Kafka instance.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "arn",
				Description: "The Alibaba Cloud Resource Name (ARN) of the Kafka instance.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getKafkaInstanceARN,
				Transform:   transform.FromValue(),
			},
			{
				Name:        "region_id",
				Description: "The region ID of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "zone_id",
				Description: "The zone ID of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "service_status",
				Description: "The status of the instance. Valid values: 0 (pending), 1 (provisioning), 2 (running), 3 (expired), 4 (idle), 5 (stopped).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "spec_type",
				Description: "The instance edition. Valid values: normal (standard), professional (professional), professionalForHighWrite (high write), serverlessStandard (serverless standard), serverlessProfessional (serverless professional), confluent (Confluent).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "deploy_type",
				Description: "The deployment type. Valid values: 1 (virtual machine), 4 (serverless), 5 (serverless).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "disk_size",
				Description: "The size of the disk (in GB).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "disk_type",
				Description: "The type of the disk. Valid values: 0 (ultra disk), 1 (SSD).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "io_max",
				Description: "The maximum IOPS of the instance.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "io_max_spec",
				Description: "The IOPS specification of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "eip_max",
				Description: "The maximum public bandwidth (in MB/s).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "msg_retain",
				Description: "The message retention period (in hours).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "topic_num_limit",
				Description: "The maximum number of topics that can be created.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "paid_type",
				Description: "The billing method. Valid values: 0 (prepaid / subscription), 1 (postpaid / pay-as-you-go).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "vpc_id",
				Description: "The ID of the VPC.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "vswitch_id",
				Description: "The ID of the vSwitch.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "security_group",
				Description: "The ID of the security group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "endpoint",
				Description: "The default endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("EndPoint"),
			},
			{
				Name:        "domain_endpoint",
				Description: "The domain name endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "ssl_endpoint",
				Description: "The SSL endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("SslEndPoint"),
			},
			{
				Name:        "ssl_domain_endpoint",
				Description: "The SSL domain endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "sasl_domain_endpoint",
				Description: "The SASL domain endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "resource_group_id",
				Description: "The ID of the resource group to which the instance belongs.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "kms_key_id",
				Description: "The ID of the KMS key used for disk encryption.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "all_config",
				Description: "The configuration of the instance in JSON format.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "create_time",
				Description: "The time when the instance was created.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreateTime").Transform(transform.UnixMsToTimestamp),
			},
			{
				Name:        "expired_time",
				Description: "The time when the instance expires.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("ExpiredTime").Transform(transform.UnixMsToTimestamp),
			},
			{
				Name:        "order_id",
				Description: "The ID of the order.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("OrderId"),
			},
			{
				Name:        "used_topic_count",
				Description: "The number of topics that have been used.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "used_group_count",
				Description: "The number of consumer groups that have been used.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "used_partition_count",
				Description: "The number of partitions that have been used.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "standard_zone_id",
				Description: "The standard zone ID of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "view_instance_status_code",
				Description: "The status code for the instance displayed in the console.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "reserved_publish_capacity",
				Description: "The reserved publish capacity.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "reserved_subscribe_capacity",
				Description: "The reserved subscribe capacity.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "upgrade_service_detail_info",
				Description: "The upgrade service detail information.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("UpgradeServiceDetailInfo"),
			},
			{
				Name:        "confluent_config",
				Description: "The Confluent configuration.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("ConfluentConfig"),
			},

			// Tags
			{
				Name:        "tags_src",
				Description: "A list of tags attached to the instance.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags.TagVO"),
			},
			{
				Name:        "tags",
				Description: "A map of tags for the resource.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags.TagVO").Transform(kafkaInstanceTags),
			},

			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.From(kafkaInstanceTitle),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     getKafkaInstanceARN,
				Transform:   transform.FromValue().Transform(ensureStringArray),
			},

			// Alicloud common columns
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

func listKafkaInstances(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service connection
	client, err := AlikafkaService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_kafka_instance.listKafkaInstances", "connection_error", err)
		return nil, err
	}

	request := alikafka.CreateGetInstanceListRequest()
	request.Scheme = "https"

	// Apply optional filters
	quals := d.EqualsQuals
	if quals["instance_id"] != nil {
		instanceID := quals["instance_id"].GetStringValue()
		request.InstanceId = &[]string{instanceID}
	}
	if quals["resource_group_id"] != nil {
		request.ResourceGroupId = quals["resource_group_id"].GetStringValue()
	}
	if quals["order_id"] != nil {
		request.OrderId = quals["order_id"].GetStringValue()
	}

	response, err := client.GetInstanceList(request)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_kafka_instance.listKafkaInstances", "query_error", err, "request", request)
		return nil, err
	}

	for _, instance := range response.InstanceList.InstanceVO {
		d.StreamListItem(ctx, instance)
		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getKafkaInstanceARN(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)
	instanceID := kafkaInstanceID(h.Item)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	return "arn:acs:alikafka:" + region + ":" + accountID + ":instance/" + instanceID, nil
}

//// TRANSFORM FUNCTIONS

func kafkaInstanceTags(_ context.Context, d *transform.TransformData) (interface{}, error) {
	tags, ok := d.Value.([]alikafka.TagVO)
	if !ok || len(tags) == 0 {
		return nil, nil
	}
	result := map[string]string{}
	for _, t := range tags {
		if t.Key != "" {
			result[t.Key] = t.Value
		}
	}
	return result, nil
}

func kafkaInstanceTitle(_ context.Context, d *transform.TransformData) (interface{}, error) {
	instance := d.HydrateItem.(alikafka.InstanceVO)
	if instance.Name != "" {
		return instance.Name, nil
	}
	return instance.InstanceId, nil
}

//// HELPER FUNCTIONS

func kafkaInstanceID(item interface{}) string {
	return item.(alikafka.InstanceVO).InstanceId
}
