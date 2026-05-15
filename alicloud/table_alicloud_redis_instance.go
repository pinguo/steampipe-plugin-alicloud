package alicloud

import (
	"context"
	"encoding/json"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/r_kvstore"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

// redisInstanceExtra holds fields returned by the API but missing from the SDK struct
type redisInstanceExtra struct {
	ShardCount       int    `json:"ShardCount"`
	ShardClass       string `json:"ShardClass"`
	ReplicaCount     int    `json:"ReplicaCount"`
	ResourceGroupId  string `json:"ResourceGroupId"`
	SecondaryZoneId  string `json:"SecondaryZoneId"`
	EditionType      string `json:"EditionType"`
	GlobalInstanceId string `json:"GlobalInstanceId"`
}

// redisInstanceRow wraps the SDK struct with extra fields parsed from raw JSON
type redisInstanceRow struct {
	r_kvstore.KVStoreInstance
	ShardCount       int    `json:"ShardCount"`
	ShardClass       string `json:"ShardClass"`
	ReplicaCount     int    `json:"ReplicaCount"`
	ResourceGroupId  string `json:"ResourceGroupId"`
	SecondaryZoneId  string `json:"SecondaryZoneId"`
	EditionType      string `json:"EditionType"`
	GlobalInstanceId string `json:"GlobalInstanceId"`
}

// redisInstanceAttrRow wraps the attribute struct with extra fields
type redisInstanceAttrRow struct {
	r_kvstore.DBInstanceAttribute
	ShardCount       int    `json:"ShardCount"`
	ShardClass       string `json:"ShardClass"`
	ReplicaCount     int    `json:"ReplicaCount"`
	ResourceGroupId  string `json:"ResourceGroupId"`
	SecondaryZoneId  string `json:"SecondaryZoneId"`
	EditionType      string `json:"EditionType"`
	GlobalInstanceId string `json:"GlobalInstanceId"`
}

//// TABLE DEFINITION

func tableAlicloudRedisInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_redis_instance",
		Description: "Alicloud Redis (R-KVStore) Instance",
		List: &plugin.ListConfig{
			Hydrate: listRedisInstances,
			Tags:    map[string]string{"service": "r-kvstore", "action": "DescribeInstances"},
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "instance_id", Require: plugin.Optional},
				{Name: "instance_status", Require: plugin.Optional},
				{Name: "instance_class", Require: plugin.Optional},
				{Name: "instance_type", Require: plugin.Optional},
				{Name: "vpc_id", Require: plugin.Optional},
				{Name: "vswitch_id", Require: plugin.Optional},
				{Name: "charge_type", Require: plugin.Optional},
				{Name: "network_type", Require: plugin.Optional},
				{Name: "architecture_type", Require: plugin.Optional},
				{Name: "zone_id", Require: plugin.Optional},
				{Name: "engine_version", Require: plugin.Optional},
				{Name: "resource_group_id", Require: plugin.Optional},
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("instance_id"),
			Hydrate:    getRedisInstance,
			Tags:       map[string]string{"service": "r-kvstore", "action": "DescribeInstanceAttribute"},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			// Top columns
			{
				Name:        "instance_id",
				Description: "The ID of the Redis instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_name",
				Description: "The name of the Redis instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "arn",
				Description: "The Alibaba Cloud Resource Name (ARN) of the Redis instance.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstanceARN,
				Transform:   transform.FromValue(),
			},
			{
				Name:        "instance_status",
				Description: "The status of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_class",
				Description: "The instance type of the Redis instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_type",
				Description: "The engine type of the instance. Valid values: Redis, Memcache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "architecture_type",
				Description: "The architecture type. Valid values: cluster, standard, rwsplit.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "node_type",
				Description: "The node type. Valid values: double, single, readone, readthree, readfive.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "engine_version",
				Description: "The engine version of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "capacity",
				Description: "The storage capacity of the instance (in MB).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "bandwidth",
				Description: "The bandwidth of the instance (in MB/s).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "connections",
				Description: "The maximum number of connections supported by the instance.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "qps",
				Description: "The queries per second (QPS) supported by the instance.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("QPS"),
			},
			{
				Name:        "shard_count",
				Description: "The number of shards in the cluster instance.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "shard_class",
				Description: "The specification of each shard in the cluster instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "replica_count",
				Description: "The number of replicas for the instance.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "charge_type",
				Description: "The billing method. Valid values: PrePaid, PostPaid.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "network_type",
				Description: "The network type. Valid values: CLASSIC, VPC.",
				Type:        proto.ColumnType_STRING,
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
				Transform:   transform.FromField("VSwitchId"),
			},
			{
				Name:        "private_ip",
				Description: "The private IP address of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "connection_domain",
				Description: "The internal endpoint of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "port",
				Description: "The service port of the instance.",
				Type:        proto.ColumnType_INT,
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
				Name:        "secondary_zone_id",
				Description: "The secondary zone ID for the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "resource_group_id",
				Description: "The ID of the resource group to which the instance belongs.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "edition_type",
				Description: "The edition type of the instance. Valid values: Community, Enterprise.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "global_instance_id",
				Description: "The ID of the distributed instance to which the instance belongs.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "config",
				Description: "The parameter settings of the instance.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "package_type",
				Description: "The plan type. Valid values: standard, customized.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "create_time",
				Description: "The time when the instance was created.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "end_time",
				Description: "The time when the subscription instance expires.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "destroy_time",
				Description: "The time when the instance was destroyed.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "has_renew_change_order",
				Description: "Indicates whether there is a pending renewal order.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "is_rds",
				Description: "Indicates whether the instance is managed by RDS.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "connection_mode",
				Description: "The connection mode of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "vpc_cloud_instance_id",
				Description: "The ID of the VPC instance.",
				Type:        proto.ColumnType_STRING,
			},

			// Attribute-only fields (from DescribeInstanceAttribute)
			{
				Name:        "engine",
				Description: "The database engine. Valid values: Redis, Memcache.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "maintain_start_time",
				Description: "The start time of the maintenance window.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "maintain_end_time",
				Description: "The end time of the maintenance window.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "availability_value",
				Description: "The availability metric of the instance.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "security_ip_list",
				Description: "The IP addresses in the whitelist.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "vpc_auth_mode",
				Description: "The VPC authentication mode.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},
			{
				Name:        "replication_mode",
				Description: "The data replication mode.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getRedisInstance,
			},

			// Tags
			{
				Name:        "tags_src",
				Description: "A list of tags attached to the instance.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags.Tag"),
			},
			{
				Name:        "tags",
				Description: "A map of tags for the resource.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags.Tag").Transform(redisInstanceTags),
			},

			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.From(redisInstanceTitle),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     getRedisInstanceARN,
				Transform:   transform.FromValue().Transform(transform.EnsureStringArray),
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

func listRedisInstances(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	// Create service connection
	client, err := RedisService(ctx, d, region)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_redis_instance.listRedisInstances", "connection_error", err)
		return nil, err
	}

	request := r_kvstore.CreateDescribeInstancesRequest()
	request.Scheme = "https"
	request.PageSize = requests.NewInteger(50)
	request.PageNumber = requests.NewInteger(1)

	// Apply optional filters
	quals := d.EqualsQuals
	if quals["instance_id"] != nil {
		request.InstanceIds = quals["instance_id"].GetStringValue()
	}
	if quals["instance_status"] != nil {
		request.InstanceStatus = quals["instance_status"].GetStringValue()
	}
	if quals["instance_class"] != nil {
		request.InstanceClass = quals["instance_class"].GetStringValue()
	}
	if quals["instance_type"] != nil {
		request.InstanceType = quals["instance_type"].GetStringValue()
	}
	if quals["vpc_id"] != nil {
		request.VpcId = quals["vpc_id"].GetStringValue()
	}
	if quals["vswitch_id"] != nil {
		request.VSwitchId = quals["vswitch_id"].GetStringValue()
	}
	if quals["charge_type"] != nil {
		request.ChargeType = quals["charge_type"].GetStringValue()
	}
	if quals["network_type"] != nil {
		request.NetworkType = quals["network_type"].GetStringValue()
	}
	if quals["architecture_type"] != nil {
		request.ArchitectureType = quals["architecture_type"].GetStringValue()
	}
	if quals["zone_id"] != nil {
		request.ZoneId = quals["zone_id"].GetStringValue()
	}
	if quals["engine_version"] != nil {
		request.EngineVersion = quals["engine_version"].GetStringValue()
	}
	if quals["resource_group_id"] != nil {
		request.ResourceGroupId = quals["resource_group_id"].GetStringValue()
	}

	count := 0
	for {
		d.WaitForListRateLimit(ctx)
		response, err := client.DescribeInstances(request)
		if err != nil {
			plugin.Logger(ctx).Error("alicloud_redis_instance.listRedisInstances", "query_error", err, "request", request)
			return nil, err
		}

		// Parse raw JSON to extract extra fields not in SDK struct
		rows, parseErr := parseRedisListResponse(response)
		if parseErr != nil {
			plugin.Logger(ctx).Warn("alicloud_redis_instance.listRedisInstances", "parse_extra_fields_error", parseErr)
			// Fallback: stream SDK structs without extra fields
			for _, instance := range response.Instances.KVStoreInstance {
				d.StreamListItem(ctx, redisInstanceRow{KVStoreInstance: instance})
				count++
				if d.RowsRemaining(ctx) == 0 {
					return nil, nil
				}
			}
		} else {
			for _, row := range rows {
				d.StreamListItem(ctx, row)
				count++
				if d.RowsRemaining(ctx) == 0 {
					return nil, nil
				}
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

func getRedisInstance(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	// Create service connection
	client, err := RedisService(ctx, d, region)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_redis_instance.getRedisInstance", "connection_error", err)
		return nil, err
	}

	var id string
	if h.Item != nil {
		id = redisInstanceID(h.Item)
	} else {
		id = d.EqualsQuals["instance_id"].GetStringValue()
	}

	request := r_kvstore.CreateDescribeInstanceAttributeRequest()
	request.Scheme = "https"
	request.InstanceId = id

	response, err := client.DescribeInstanceAttribute(request)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_redis_instance.getRedisInstance", "query_error", err, "request", request)
		return nil, err
	}

	if response.Instances.DBInstanceAttribute == nil || len(response.Instances.DBInstanceAttribute) == 0 {
		return nil, nil
	}

	if response.Instances.DBInstanceAttribute[0].RegionId != region {
		return nil, nil
	}

	// Parse raw JSON to get extra fields
	row, parseErr := parseRedisAttrResponse(response, 0)
	if parseErr != nil {
		plugin.Logger(ctx).Warn("alicloud_redis_instance.getRedisInstance", "parse_extra_fields_error", parseErr)
		return redisInstanceAttrRow{DBInstanceAttribute: response.Instances.DBInstanceAttribute[0]}, nil
	}
	return row, nil
}

func getRedisInstanceARN(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := redisInstanceRegion(h.Item)
	instanceID := redisInstanceID(h.Item)

	// Get project details
	getCommonColumnsCached := plugin.HydrateFunc(getCommonColumns).WithCache()
	commonData, err := getCommonColumnsCached(ctx, d, h)
	if err != nil {
		return nil, err
	}
	commonColumnData := commonData.(*alicloudCommonColumnData)
	accountID := commonColumnData.AccountID

	return "arn:acs:r-kvstore:" + region + ":" + accountID + ":instance/" + instanceID, nil
}

//// JSON PARSING HELPERS

// parseRedisListResponse parses the raw HTTP response to extract extra fields
func parseRedisListResponse(response *r_kvstore.DescribeInstancesResponse) ([]redisInstanceRow, error) {
	httpContent := response.GetHttpContentString()
	if httpContent == "" {
		// No raw content available, return SDK structs with zero extra fields
		rows := make([]redisInstanceRow, len(response.Instances.KVStoreInstance))
		for i, inst := range response.Instances.KVStoreInstance {
			rows[i] = redisInstanceRow{KVStoreInstance: inst}
		}
		return rows, nil
	}

	var rawResp struct {
		Instances struct {
			KVStoreInstance []json.RawMessage `json:"KVStoreInstance"`
		} `json:"Instances"`
	}
	if err := json.Unmarshal([]byte(httpContent), &rawResp); err != nil {
		return nil, err
	}

	rows := make([]redisInstanceRow, len(response.Instances.KVStoreInstance))
	for i, inst := range response.Instances.KVStoreInstance {
		rows[i] = redisInstanceRow{KVStoreInstance: inst}
		if i < len(rawResp.Instances.KVStoreInstance) {
			var extra redisInstanceExtra
			if err := json.Unmarshal(rawResp.Instances.KVStoreInstance[i], &extra); err == nil {
				rows[i].ShardCount = extra.ShardCount
				rows[i].ShardClass = extra.ShardClass
				rows[i].ReplicaCount = extra.ReplicaCount
				rows[i].ResourceGroupId = extra.ResourceGroupId
				rows[i].SecondaryZoneId = extra.SecondaryZoneId
				rows[i].EditionType = extra.EditionType
				rows[i].GlobalInstanceId = extra.GlobalInstanceId
			}
		}
	}
	return rows, nil
}

// parseRedisAttrResponse parses the raw HTTP response for DescribeInstanceAttribute
func parseRedisAttrResponse(response *r_kvstore.DescribeInstanceAttributeResponse, index int) (redisInstanceAttrRow, error) {
	row := redisInstanceAttrRow{DBInstanceAttribute: response.Instances.DBInstanceAttribute[index]}

	httpContent := response.GetHttpContentString()
	if httpContent == "" {
		return row, nil
	}

	var rawResp struct {
		Instances struct {
			DBInstanceAttribute []json.RawMessage `json:"DBInstanceAttribute"`
		} `json:"Instances"`
	}
	if err := json.Unmarshal([]byte(httpContent), &rawResp); err != nil {
		return row, err
	}

	if index < len(rawResp.Instances.DBInstanceAttribute) {
		var extra redisInstanceExtra
		if err := json.Unmarshal(rawResp.Instances.DBInstanceAttribute[index], &extra); err == nil {
			row.ShardCount = extra.ShardCount
			row.ShardClass = extra.ShardClass
			row.ReplicaCount = extra.ReplicaCount
			row.ResourceGroupId = extra.ResourceGroupId
			row.SecondaryZoneId = extra.SecondaryZoneId
			row.EditionType = extra.EditionType
			row.GlobalInstanceId = extra.GlobalInstanceId
		}
	}
	return row, nil
}

//// TRANSFORM FUNCTIONS

func redisInstanceTags(_ context.Context, d *transform.TransformData) (interface{}, error) {
	tags, ok := d.Value.([]r_kvstore.Tag)
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

func redisInstanceTitle(_ context.Context, d *transform.TransformData) (interface{}, error) {
	switch item := d.HydrateItem.(type) {
	case redisInstanceRow:
		if item.InstanceName != "" {
			return item.InstanceName, nil
		}
		return item.InstanceId, nil
	case redisInstanceAttrRow:
		if item.InstanceName != "" {
			return item.InstanceName, nil
		}
		return item.InstanceId, nil
	}
	return nil, nil
}

//// HELPER FUNCTIONS

func redisInstanceID(item interface{}) string {
	switch i := item.(type) {
	case redisInstanceRow:
		return i.InstanceId
	case redisInstanceAttrRow:
		return i.InstanceId
	}
	return ""
}

func redisInstanceRegion(item interface{}) string {
	switch i := item.(type) {
	case redisInstanceRow:
		return i.RegionId
	case redisInstanceAttrRow:
		return i.RegionId
	}
	return ""
}
